# broker-crash-comparison

At a fixed publish rate, what do Kafka, NSQ and RabbitMQ actually cost you — throughput,
end-to-end latency, and what happens to in-flight messages when the consumer is SIGKILLed
mid-stream? The delivery guarantees are documented. The size of the duplicate burst and the
redelivery delay are not, because they are a function of how you acknowledge, and that is what
this measures.

Headline table: see [WRITEUP.mdx](WRITEUP.mdx). Full per-run numbers: [results/summary.md](results/summary.md),
raw logs under [results/runs/](results/runs/). Decision and the honest durability caveats:
[docs/adr/0001-broker-durability-and-crash-comparison.md](docs/adr/0001-broker-durability-and-crash-comparison.md).

## What is measured

- **Fixed publish rate**, 2000 msg/s of 512-byte fixed-seed payloads for 30s, identical for all
  three brokers. Saturation of the publish path is a separate, secondary number
  (`make saturation` → `results/saturation.txt`).
- **Two arms per broker.** _control_: nothing fails. _crash_: `docker kill -s SIGKILL` the
  consumer container at t=12s, `docker start` it at t=17s, same timing for every broker.
- **Lost and duplicate are set differences** over monotonic sequence ids, never estimates. The
  producer logs `seq,publishNanos`; the consumer appends `seq,receiveNanos` per delivery,
  unbuffered, so work already done survives the kill.
- **Latency** is publish-to-consume across two processes and two clocks. The container-minus-host
  offset is measured immediately before each run and subtracted. See the ADR for the caveat.
- **Backlog** is published-minus-uniquely-consumed at each event, computed from the logs — the
  same arithmetic for all three brokers, rather than three different metrics endpoints.
- 4 runs per arm; **run 1 is discarded as warm-up**, the reported figure is the median of the
  rest.

## Durability, stated

| broker          | producer                     | broker                                                                | consumer ack                         |
| --------------- | ---------------------------- | --------------------------------------------------------------------- | ------------------------------------ |
| Kafka 3.9 KRaft | `acks=all`, synchronous      | RF=1, `min.insync.replicas=1`, default flush (page cache)             | offsets committed on a 1s interval   |
| NSQ 1.3         | synchronous `PUB`            | `--mem-queue-size=0` (disk queue), `--sync-every` at its 2500 default | `FIN` per message, max-in-flight 256 |
| RabbitMQ 4      | publisher confirms, blocking | durable queue, persistent messages                                    | `ack` per message, prefetch 256      |

Kafka with RF=1 and the default flush policy is the weakest of the three on disk durability, and
the write-up says so rather than hiding it. Read the ADR before quoting any number.

## Machine

Apple M1 Pro, 8 cores, 16 GB RAM, macOS (Darwin 25.6.0). Docker via OrbStack 29.4.0, Linux VM
capped at 8 CPU / 8 GB RAM. Load generator (the producer) runs on the host and targets containers
over `localhost`; the consumer runs in a container so it can be SIGKILLed. Go 1.27.

## Reproduce cold

```bash
git clone https://github.com/dhiazfathra/broker-crash-comparison
cd broker-crash-comparison
make bench          # builds, then runs every broker x arm x rep, writes results/summary.md
```

`make bench` brings each broker up and down itself; no `docker compose up -d` is needed first.
On a shared machine, hold the benchmark mutex around it and never release a lock you do not own:

```bash
/tmp/expbrief/benchlock.sh acquire broker-crash-comparison
make bench
make saturation
/tmp/expbrief/benchlock.sh release broker-crash-comparison
```

Other targets: `make test` (unit tests), `make lint` (golangci-lint), `make clean`
(`docker compose down -v`).

Knobs, all environment variables on `scripts/run.sh`: `RATE`, `DUR`, `REPS`, `KILL_AT`, `PAUSE`,
`BROKERS`, `ARMS`.

## Ports

The compose project is named `brokercrash` and binds host ports 19092 (Kafka), 14150/14151 (NSQ)
and 5673 (RabbitMQ) so it cannot collide with the other benchmark stacks on this machine.

## License

MIT — see [LICENSE](LICENSE).
