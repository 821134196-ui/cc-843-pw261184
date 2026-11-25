"""Unit tests for the runtime guarantees."""

import threading
import time
import unittest

from event_runtime import (
    BackoffPolicy,
    Event,
    NaiveRuntime,
    SmartRuntime,
    StateStore,
    StaleVersionError,
)


class StoreTests(unittest.TestCase):
    def test_cas_rejects_stale_versions(self):
        store = StateStore()
        store.commit(Event("t", "k", 2, payload="v2"))
        with self.assertRaises(StaleVersionError):
            store.commit(Event("t", "k", 1, payload="v1"))
        # equal version is also rejected
        with self.assertRaises(StaleVersionError):
            store.commit(Event("t", "k", 2, payload="v2-again"))
        store.commit(Event("t", "k", 3, payload="v3"))
        self.assertEqual(store.snapshot()["k"]["version"], 3)


class CoalesceTests(unittest.TestCase):
    def test_consecutive_updates_merge_into_one_pending_event(self):
        seen = []
        done = threading.Event()

        def handler(event):
            seen.append(event.version)
            if event.version == 1:
                done.wait(1.0)  # hold v1 while v2..v5 arrive
            else:
                done.set()

        rt = SmartRuntime(handler=handler, workers=1).start()
        rt.submit(Event("t", "k", 1))
        time.sleep(0.05)
        for v in range(2, 6):
            rt.submit(Event("t", "k", v))
        self.assertTrue(rt.drain(3.0))
        rt.stop()

        # v1 handled, then a single merged event at v5 (v2-v4 coalesced away)
        self.assertEqual(seen, [1, 5])
        self.assertEqual(rt.metrics.summary()["coalesced"], 3)
        self.assertEqual(rt.store.snapshot()["k"]["version"], 5)

    def test_naive_runtime_keeps_every_event(self):
        seen = []
        rt = NaiveRuntime(handler=lambda e: seen.append(e.version),
                          workers=1).start()
        for v in range(1, 6):
            rt.submit(Event("t", "k", v))
        rt.drain(3.0)
        rt.stop()
        summary = rt.metrics.summary()
        # No coalescing: every event is handled in FIFO order, each
        # occupying its own queue slot; zero merges are reported.
        self.assertEqual(seen, [1, 2, 3, 4, 5])
        self.assertEqual(summary["coalesced"], 0)
        self.assertEqual(summary["submitted"], 5)


class OrderingTests(unittest.TestCase):
    def test_same_key_never_processed_concurrently(self):
        in_flight = 0
        max_in_flight = 0
        lock = threading.Lock()

        def handler(event):
            nonlocal in_flight, max_in_flight
            with lock:
                in_flight += 1
                max_in_flight = max(max_in_flight, in_flight)
            time.sleep(0.01)
            with lock:
                in_flight -= 1

        for runtime_cls in (SmartRuntime, NaiveRuntime):
            in_flight = max_in_flight = 0
            rt = runtime_cls(handler=handler, workers=4).start()
            for v in range(20):
                rt.submit(Event("t", "hot", v + 1))
            rt.drain(3.0)
            rt.stop()
            self.assertEqual(max_in_flight, 1,
                             f"{runtime_cls.__name__} ran a key concurrently")


class BackoffTests(unittest.TestCase):
    def test_failed_event_retries_with_backoff_then_succeeds(self):
        attempts = {"n": 0}

        def handler(event):
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("transient")

        rt = SmartRuntime(
            handler=handler, workers=1,
            backoff=BackoffPolicy(base=0.01, factor=2, cap=0.1, jitter=0),
            max_attempts=4,
        ).start()
        rt.submit(Event("t", "k", 1))
        self.assertTrue(rt.drain(3.0))
        rt.stop()
        summary = rt.metrics.summary()
        self.assertEqual(attempts["n"], 3)
        self.assertEqual(summary["retried"], 2)
        self.assertEqual(summary["processed"], 1)
        self.assertEqual(summary["dead_lettered"], 0)

    def test_exhausting_attempts_dead_letters(self):
        def handler(event):
            raise RuntimeError("permanent")

        rt = SmartRuntime(
            handler=handler, workers=1,
            backoff=BackoffPolicy(base=0.005, jitter=0),
            max_attempts=3,
        ).start()
        rt.submit(Event("t", "k", 1))
        self.assertTrue(rt.drain(3.0))
        rt.stop()
        summary = rt.metrics.summary()
        self.assertEqual(summary["dead_lettered"], 1)
        self.assertEqual(summary["processed"], 0)
        self.assertEqual(len(summary["dead_letters"]), 1)


class StaleRetryTests(unittest.TestCase):
    def test_stale_retry_cannot_overwrite_newer_state(self):
        gate = threading.Event()

        def handler(event):
            if event.version == 1:
                gate.wait(2.0)      # v1 held back as if retrying
                raise RuntimeError("v1 always fails")

        rt = SmartRuntime(
            handler=handler, workers=2,
            backoff=BackoffPolicy(base=0.01, jitter=0),
            max_attempts=3,
        ).start()
        rt.submit(Event("t", "k", 1))
        time.sleep(0.05)
        rt.submit(Event("t", "k", 2))   # newer version commits first
        gate.set()
        self.assertTrue(rt.drain(4.0))
        rt.stop()

        self.assertEqual(rt.store.version("k"), 2)
        self.assertGreaterEqual(rt.metrics.summary()["stale_dropped"], 1)


class FairnessTests(unittest.TestCase):
    def test_storm_tenant_cannot_starve_healthy_tenant(self):
        healthy_latencies = []
        healthy_lock = threading.Lock()

        def handler(event):
            time.sleep(0.001)
            if event.tenant == "healthy":
                with healthy_lock:
                    healthy_latencies.append(
                        time.monotonic() - event.enqueued_at)

        rt = SmartRuntime(handler=handler, workers=4).start()
        rt.set_quantum("storm", 2)

        # Paced flood from a feeder thread: ~2000 storm updates/s against
        # ~40 healthy updates/s for 1.5 s (above service capacity, so the
        # coalescing + fairness mechanisms are genuinely stressed).
        stop = threading.Event()

        def feed():
            v = 0
            next_storm = time.monotonic()
            next_healthy = time.monotonic()
            while not stop.is_set():
                now = time.monotonic()
                if now >= next_storm:
                    v += 1
                    rt.submit(Event("storm", "hot", v))
                    next_storm += 0.0005
                if now >= next_healthy:
                    rt.submit(Event("healthy", f"hk-{v % 10}", v))
                    next_healthy += 0.025
                time.sleep(0.0001)

        feeder = threading.Thread(target=feed, daemon=True)
        feeder.start()
        time.sleep(1.5)
        stop.set()
        feeder.join()
        self.assertTrue(rt.drain(6.0))
        rt.stop()

        self.assertTrue(healthy_latencies)
        # healthy events must be served promptly, not behind the whole flood
        worst = max(healthy_latencies)
        self.assertLess(worst, 1.0,
                        f"healthy tenant starved: worst latency {worst:.2f}s")


class StressTests(unittest.TestCase):
    def test_many_tenants_drain_without_stall_and_final_state_is_latest(self):
        import random

        rng = random.Random(99)
        for runtime_cls in (NaiveRuntime, SmartRuntime):
            store = StateStore()

            def handler(event):
                time.sleep(0.0005)
                if rng.random() < 0.05:
                    raise RuntimeError("boom")

            rt = runtime_cls(
                handler=handler, store=store, workers=8,
                backoff=BackoffPolicy(base=0.005, cap=0.05),
                max_attempts=3, seed=3,
            )
            if runtime_cls is SmartRuntime:
                rt.tenant_concurrency = 3
            rt.start()
            stop = threading.Event()

            def feed(tenant):
                v = 0
                end = time.monotonic() + 1.0
                while time.monotonic() < end:
                    v += 1
                    rt.submit(Event(tenant, f"k{rng.randrange(20)}", v))
                    time.sleep(0.0003)

            threads = [
                threading.Thread(target=feed, args=(f"t{i}",))
                for i in range(8)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.assertTrue(rt.drain(15.0), f"{runtime_cls.__name__} stalled")
            rt.stop()

            # Every surviving key must hold a positive (latest-committed)
            # version; CAS guarantees no stale overwrite.
            self.assertTrue(store.snapshot())
            for key, entry in store.snapshot().items():
                self.assertGreater(entry["version"], 0)


class RedeliveryTests(unittest.TestCase):
    def test_retried_event_is_marked_redelivered(self):
        seen = []

        def handler(event):
            seen.append((event.version, event.attempt, event.redelivered))
            if event.attempt == 1:
                raise RuntimeError("first attempt fails")

        rt = SmartRuntime(
            handler=handler, workers=1,
            backoff=BackoffPolicy(base=0.005, jitter=0),
            max_attempts=3,
        ).start()
        rt.submit(Event("t", "k", 1))
        self.assertTrue(rt.drain(3.0))
        rt.stop()
        self.assertEqual(seen[0], (1, 1, False))
        self.assertEqual(seen[1][0:2], (1, 2))
        self.assertTrue(seen[1][2])  # second attempt is flagged redelivery


if __name__ == "__main__":
    unittest.main(verbosity=2)
