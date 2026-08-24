# ADR 0001: How the three brokers are configured for a like-for-like crash comparison

## Status

Accepted

## Date

2026-08-24

## Context

We wanted one number-backed answer to "Kafka vs NSQ vs RabbitMQ at a fixed publish rate":
throughput, end-to-end latency, and what a consumer SIGKILL actually costs in duplicates and
redelivery delay. The delivery guarantees are documented; the *size* of the duplicate burst is
not, because it is a function of the client's acknowledgement configuration, not of the broker's
protocol.

The comparison is only worth anything if the durability settings are comparable and stated.
A Kafka-durable vs NSQ-in-memory benchmark is not a comparison.

## Decision

Single-node each, one partition/queue/channel, 512-byte fixed-seed payloads, monotonic sequence
ids, one Go harness with one `Producer`/`Consumer` interface per broker
(`internal/broker/`). Producer on the host, consumer in a container so it can be SIGKILLed.

Durability, as configured, honestly:

| broker | producer side | broker side | consumer ack |
| --- | --- | --- | --- |
| Kafka 3.9 (KRaft) | `acks=all`, synchronous write | RF=1, `min.insync.replicas=1`, **default flush policy (page cache, no per-message fsync)** | offsets committed on a 1s interval (kafka-go `CommitInterval`) |
| NSQ 1.3 | synchronous `PUB`, waits for `OK` | `--mem-queue-size=0` so every message goes to the disk queue; `--sync-every` left at its 2500-message default | `FIN` per message, `max-in-flight=256` |
| RabbitMQ 4 | publisher confirms, publish blocks on the confirm | durable queue + `delivery_mode=2` persistent | manual `ack` per message, prefetch 256 |

The asymmetry that remains, and that we chose not to paper over: with RF=1 and the default flush
policy, Kafka `acks=all` means "the leader has it in page cache", **not** "it is on disk". NSQ at
`--sync-every=2500` and RabbitMQ persistent+confirms both batch their fsyncs, so they are closer
to each other than either is to Kafka. Forcing `flush.messages=1` on Kafka would have equalised
the fsync story and produced a Kafka number nobody would recognise, because nobody runs Kafka
that way — real Kafka durability comes from replication, which a single node cannot provide.

Crash injection is identical for all three: `docker kill -s SIGKILL` the consumer container 12s
into a 30s run, restart it 5s later, and let the run drain. A clean control arm runs per broker.

## Alternatives considered

- **Force fsync-per-message everywhere** (`flush.messages=1`, `--sync-every=1`, and Rabbit's
  quorum queues). Rejected: it equalises a knob nobody turns, and it measures the laptop's fsync
  latency rather than the brokers.
- **Multi-node Kafka / quorum queues / a real cluster.** Rejected on this machine: 8 GiB of VM
  RAM and ~9 GiB of free disk shared with six other experiments. The consequence is stated below.
- **Kill the broker instead of the consumer.** Rejected: it would measure broker recovery, and
  with RF=1 Kafka's answer would be "data loss", which is a property of the deployment we were
  forced into, not a finding.
- **k6 as the load generator.** Rejected: none of the three speak HTTP, and a Go harness lets the
  producer and consumer share one payload codec so lost/duplicate are set differences.
- **Broker-reported queue depth for backlog.** Rejected: three different metrics endpoints with
  three different sampling behaviours. Backlog is computed from the logs as
  published-minus-uniquely-consumed at each event, which is the same arithmetic for all three.

## Consequences

- The durability arm measures the **local write path**, not replication. Nothing here says
  anything about what Kafka does when a broker dies in a 3-node cluster, which is the only
  configuration anyone should run.
- Kafka's duplicate burst is a property of `CommitInterval=1s`, not of Kafka. Commit per message
  and the burst shrinks toward zero while throughput collapses. The number is a measurement of a
  *choice*, and the write-up says so.
- End-to-end latency crosses a process and a container boundary, so it crosses two clocks. We
  measure the container-minus-host offset immediately before each run and subtract it; residual
  drift over a 30s run is not corrected for.
- One partition means Kafka is measured without the parallelism that is its main answer to
  throughput pressure. This favours NSQ and RabbitMQ on throughput and is a deliberate
  single-consumer comparison.
- Six other benchmark stacks share the machine, so all runs are serialised behind a filesystem
  lock. If the lock discipline is broken by another agent, the numbers are garbage and would need
  re-running.
