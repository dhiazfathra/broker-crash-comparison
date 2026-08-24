#!/usr/bin/env bash
# Secondary number: publish-path saturation per broker, no consumer attached.
# Ramps the offered rate until the achieved rate stops tracking it.
set -euo pipefail
cd "$(dirname "$0")/.."
RATES=${RATES:-"2000 5000 10000 20000 40000"}
DUR=${DUR:-10}
svc_of()  { case $1 in kafka) echo kafka;; nsq) echo nsqd;; rabbit) echo rabbit;; esac; }
hostaddr(){ case $1 in kafka) echo localhost:19092;; nsq) echo localhost:14150;; rabbit) echo amqp://bench:bench@localhost:5673/;; esac; }
hostport(){ case $1 in kafka) echo 19092;; nsq) echo 14150;; rabbit) echo 5673;; esac; }

while ! mkdir /tmp/bench.lock 2>/dev/null; do echo "waiting for benchmark lock..."; sleep 20; done
trap 'rmdir /tmp/bench.lock 2>/dev/null || true' EXIT

mkdir -p results
: > results/saturation.txt
for b in kafka nsq rabbit; do
  docker compose down -v >/dev/null 2>&1 || true
  docker compose up -d "$(svc_of "$b")" >/dev/null
  for _ in $(seq 1 90); do (exec 3<>/dev/tcp/127.0.0.1/"$(hostport "$b")") 2>/dev/null && break; sleep 1; done
  sleep 8
  [ "$b" = kafka ] && docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic bench --partitions 1 --replication-factor 1 >/dev/null
  for r in $RATES; do
    out=$(./bin/producer -broker "$b" -addr "$(hostaddr "$b")" -topic bench -rate "$r" \
      -duration "${DUR}s" -workers 64 -out /tmp/sat-produced.csv)
    echo "$b $out" | tee -a results/saturation.txt
  done
done
docker compose down -v >/dev/null 2>&1 || true
