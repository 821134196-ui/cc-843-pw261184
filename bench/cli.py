"""CLI: inject the fixed workload into both runtimes and print a comparison.

Usage:
    python -m bench.cli [--json]
"""

from __future__ import annotations

import argparse
import json

from .runner import compare
from .workload import WorkloadConfig


def _fmt_ms(value: float) -> str:
    return f"{value:8.2f} ms"


def print_report(report: dict) -> None:
    wl = report["workload"]
    print("=" * 78)
    print("固定负载 (fixed workload)")
    print("=" * 78)
    print(f"  事件总数: {wl['events']}    注入时长: {wl['duration']}s")
    print(f"  风暴突发: {wl['burst_size']} 连发 / 间隔 "
          f"{wl['burst_gap']*1000:.0f} ms    "
          f"健康租户间隔: {wl['healthy_interval']*1000:.0f} ms")
    print(f"  租户: {', '.join(wl['tenants'])}")
    print()

    headers = ["指标", "naive-fifo 基线", "smart 合并+DRR"]
    print(f"{headers[0]:<26}{headers[1]:>24}{headers[2]:>24}")
    print("-" * 78)

    naive, smart = report["runs"]
    nm, sm = naive["metrics"], smart["metrics"]

    rows = [
        ("提交事件", nm["submitted"], sm["submitted"]),
        ("成功处理", nm["processed"], sm["processed"]),
        ("合并的冗余更新", nm["coalesced"], sm["coalesced"]),
        ("重试次数", nm["retried"], sm["retried"]),
        ("过期版本丢弃", nm["stale_dropped"], sm["stale_dropped"]),
        ("死信", nm["dead_lettered"], sm["dead_lettered"]),
        ("峰值总积压", naive["backlog"]["peak_total"],
         smart["backlog"]["peak_total"]),
        ("峰值排队积压", naive["backlog"]["peak_queued"],
         smart["backlog"]["peak_queued"]),
        ("平均总积压", naive["backlog"]["avg_total"],
         smart["backlog"]["avg_total"]),
    ]
    for name, a, b in rows:
        print(f"{name:<26}{a:>24}{b:>24}")

    print()
    print("端到端处理延迟 (enqueue -> commit):")
    print(f"{'':<26}{'naive':>24}{'smart':>24}")
    for pct in ("mean", "p50", "p95", "p99", "max"):
        print(f"{pct:<26}"
              f"{_fmt_ms(nm['latency_ms'][pct]):>24}"
              f"{_fmt_ms(sm['latency_ms'][pct]):>24}")

    print()
    print("按租户的 p95 延迟 (风暴不得饿死健康租户):")
    tenants = sorted(set(nm["tenant_latency_ms"]) |
                     set(sm["tenant_latency_ms"]))
    print(f"{'tenant':<16}{'naive p95':>16}{'smart p95':>16}")
    for tenant in tenants:
        a = nm["tenant_latency_ms"].get(tenant, {}).get("p95", 0.0)
        b = sm["tenant_latency_ms"].get(tenant, {}).get("p95", 0.0)
        print(f"{tenant:<16}{a:>13.2f} ms{b:>13.2f} ms")

    print()
    print("正确性校验:")
    for run in report["runs"]:
        m = run["metrics"]
        checks = {
            "队列已排空": run["drained"],
            "过期重试被丢弃(>=1)": m["stale_dropped"] >= 1,
            "死信仅来自永久故障键(==1)": m["dead_lettered"] == 1,
            "无成功事件丢失": m["processed"] + m["stale_dropped"]
            + m["dead_lettered"] <= m["submitted"],
        }
        ok = all(checks.values())
        verdict = "PASS" if ok else "FAIL"
        print(f"  [{verdict}] {run['label']}: "
              f"过期丢弃={m['stale_dropped']}, 死信={m['dead_lettered']}")
        for name, passed in checks.items():
            print(f"        {'✓' if passed else '✗'} {name}")
    print("=" * 78)


def main() -> None:
    parser = argparse.ArgumentParser(description="Event runtime benchmark")
    parser.add_argument("--json", action="store_true",
                        help="emit machine-readable JSON instead of text")
    args = parser.parse_args()

    report = compare(WorkloadConfig())
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        print_report(report)


if __name__ == "__main__":
    main()
