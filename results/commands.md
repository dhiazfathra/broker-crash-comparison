# Commands that produced everything in this directory

Machine: Apple M1 Pro, 8 cores, 16 GB RAM, macOS (Darwin 25.6.0). Docker via OrbStack 29.4.0,
Linux VM capped at 8 CPU / 8 GB RAM. Producer on the host, consumer in a container. Go 1.27.

Six other benchmark stacks shared this machine, so the whole matrix was measured in one pass
while holding the shared mutex:

```bash
/tmp/expbrief/benchlock.sh acquire broker-crash-comparison
./scripts/run.sh          # == make bench, after make build
./scripts/saturation.sh   # == make saturation
/tmp/expbrief/benchlock.sh release broker-crash-comparison
```

Defaults in effect for `results/runs/`: `RATE=2000`, `DUR=30`, `REPS=4`, `KILL_AT=12`, `PAUSE=5`,
brokers `kafka nsq rabbit`, arms `control crash`. Run 1 of each arm is warm-up and is dropped by
`scripts/summarise.py`, which wrote `results/summary.md`.

`results/saturation.txt` came from `scripts/saturation.sh` with `RATES="2000 5000 10000 20000
40000"`, `DUR=10`, 64 publish slots, and **no consumer attached** — it measures the publish path
only.

Per run, `results/runs/` holds:

- `<broker>-<arm>-run<N>.json` — the computed metrics (`cmd/analyze`)
- `<broker>-<arm>-run<N>.produced.csv.gz` — `seq,publishNanos` from the producer
- `<broker>-<arm>-run<N>.consumed.csv.gz` — `seq,receiveNanos` per delivery from the consumer,
  container clock, corrected by the per-run offset printed in the run log
- `<broker>-<arm>-run<N>.stats.csv` — `hostNanos,name,cpu%,memUsage` sampled every 3s
- `<broker>-<arm>-run<N>.producer.txt` — offered vs achieved rate and publish failures

## Runs that were discarded and why

An earlier pass of the crash arm reported ~35,000 lost messages for Kafka. That was a harness
defect, not Kafka: the drain loop stopped as soon as the consumed count went still, which for
Kafka happens _during_ the group rebalance, before the SIGKILLed member's 30s session timeout
expires. The crash arm now waits at least 60s after the producer finishes. Those runs are not in
this directory.
