#!/usr/bin/env python3
"""Median over runs 2..N (run 1 is warm-up and is dropped) -> results/summary.md."""
import glob, json, os, re, statistics as st, collections

runs = collections.defaultdict(list)
for p in sorted(glob.glob("results/runs/*.json")):
    r = json.load(open(p))
    if r["run"] == 1:
        continue
    runs[(r["broker"], r["arm"])].append(r)

def med(rs, k):
    return st.median([x[k] for x in rs])

def steady(broker, arm):
    """Median steady-state CPU% and RSS MiB of the broker container."""
    cpu, rss = [], []
    for p in glob.glob(f"results/runs/{broker}-{arm}-run*.stats.csv"):
        if re.search(r"run1\.", p):
            continue
        for line in open(p):
            f = line.strip().split(",")
            if len(f) < 4:
                continue
            try:
                cpu.append(float(f[2].rstrip("%")))
                rss.append(float(re.match(r"([\d.]+)([A-Za-z]+)", f[3]).group(1)) *
                           {"KiB": 1/1024, "MiB": 1, "GiB": 1024}[re.match(r"[\d.]+([A-Za-z]+)", f[3]).group(1)])
            except Exception:
                pass
    return (st.median(cpu) if cpu else 0.0, st.median(rss) if rss else 0.0)

print("# Summary (median of runs 2..N, run 1 discarded as warm-up)\n")
hdr = ("| broker | arm | n | published | delivered | lost | duplicates | dups after restart | "
       "thr msg/s | p50 ms | p95 ms | p99 ms | max ms | backlog peak | redeliv delay ms | "
       "catch-up ms | broker CPU % | broker RSS MiB |")
print(hdr)
print("|" + "---|" * (hdr.count("|") - 1))
for (b, arm), rs in sorted(runs.items()):
    cpu, rss = steady(b, arm)
    print(f"| {b} | {arm} | {len(rs)} | {med(rs,'published'):.0f} | {med(rs,'deliveries'):.0f} | "
          f"{med(rs,'lost'):.0f} | {med(rs,'duplicates'):.0f} | {med(rs,'duplicates_after_restart'):.0f} | "
          f"{med(rs,'throughput_msgs_per_sec'):.0f} | {med(rs,'latency_p50_ms'):.1f} | "
          f"{med(rs,'latency_p95_ms'):.1f} | {med(rs,'latency_p99_ms'):.1f} | {med(rs,'latency_max_ms'):.0f} | "
          f"{med(rs,'backlog_peak'):.0f} | {med(rs,'redelivery_delay_ms'):.0f} | "
          f"{med(rs,'catch_up_ms'):.0f} | {cpu:.1f} | {rss:.0f} |")
