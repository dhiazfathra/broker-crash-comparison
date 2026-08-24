#!/usr/bin/env bash
# One command: every broker, both arms, REPS runs each. Run 1 is warm-up and is
# discarded by scripts/summarise.py.
set -euo pipefail
cd "$(dirname "$0")/.."

RATE=${RATE:-2000}
DUR=${DUR:-30}
REPS=${REPS:-4}
KILL_AT=${KILL_AT:-12}
PAUSE=${PAUSE:-5}
BROKERS=${BROKERS:-"kafka nsq rabbit"}
ARMS=${ARMS:-"control crash"}
CONSUMER_CT=brokercrash-consumer-1

now_ns() { python3 -c 'import time;print(time.time_ns())'; }

svc_of()  { case $1 in kafka) echo kafka;; nsq) echo nsqd;; rabbit) echo rabbit;; esac; }
inaddr()  { case $1 in kafka) echo kafka:9092;; nsq) echo nsqd:4150;; rabbit) echo amqp://bench:bench@rabbit:5672/;; esac; }
hostaddr(){ case $1 in kafka) echo localhost:19092;; nsq) echo localhost:14150;; rabbit) echo amqp://bench:bench@localhost:5673/;; esac; }
hostport(){ case $1 in kafka) echo 19092;; nsq) echo 14150;; rabbit) echo 5673;; esac; }

wait_port() {
  for _ in $(seq 1 90); do
    if (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null; then exec 3<&- ; return 0; fi
    sleep 1
  done
  echo "port $1 never opened" >&2; exit 1
}

mkdir -p results/runs

while ! mkdir /tmp/bench.lock 2>/dev/null; do echo "waiting for benchmark lock..."; sleep 20; done
trap 'rmdir /tmp/bench.lock 2>/dev/null || true' EXIT

for b in $BROKERS; do
  for arm in $ARMS; do
    for run in $(seq 1 "$REPS"); do
      tag="$b-$arm-run$run"
      echo "=== $tag ==="
      docker compose down -v >/dev/null 2>&1 || true
      rm -rf results/current && mkdir -p results/current

      docker compose up -d "$(svc_of "$b")" >/dev/null
      wait_port "$(hostport "$b")"
      sleep 8
      if [ "$b" = kafka ]; then
        docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
          --create --topic bench --partitions 1 --replication-factor 1 >/dev/null
      fi

      BROKER="$b" BROKER_ADDR="$(inaddr "$b")" docker compose up -d consumer >/dev/null
      sleep 3

      h1=$(now_ns); c=$(docker exec "$CONSUMER_CT" /app/consumer -clock); h2=$(now_ns)
      offset=$(( c - (h1 + h2) / 2 ))
      echo "clock offset (container-host) = ${offset} ns"

      ( while true; do
          printf '%s,' "$(now_ns)"
          docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' "brokercrash-$(svc_of "$b")-1" 2>/dev/null || echo
          sleep 3
        done ) > results/current/stats.csv &
      statspid=$!

      ./bin/producer -broker "$b" -addr "$(hostaddr "$b")" -topic bench -rate "$RATE" \
        -duration "${DUR}s" -out results/current/produced.csv > results/current/producer.log 2>&1 &
      prodpid=$!

      restart_ns=0
      if [ "$arm" = crash ]; then
        sleep "$KILL_AT"
        kill_ns=$(now_ns); docker kill -s SIGKILL "$CONSUMER_CT" >/dev/null
        sleep "$PAUSE"
        docker start "$CONSUMER_CT" >/dev/null; restart_ns=$(now_ns)
        echo "killed at $kill_ns, restarted at $restart_ns"
      fi
      wait $prodpid || true
      cat results/current/producer.log

      # drain: wait until the consumed line count stops moving
      prev=-1
      for _ in $(seq 1 40); do
        sleep 3
        cur=$(wc -l < results/current/consumed.csv 2>/dev/null || echo 0)
        [ "$cur" = "$prev" ] && break
        prev=$cur
      done
      kill $statspid 2>/dev/null || true
      docker stop -t 3 "$CONSUMER_CT" >/dev/null 2>&1 || true

      ./bin/analyze -produced results/current/produced.csv -consumed results/current/consumed.csv \
        -offset-ns "$offset" -restart-ns "$restart_ns" -broker "$b" -arm "$arm" -run "$run" \
        > "results/runs/$tag.json"
      cat "results/runs/$tag.json"
      cp results/current/stats.csv "results/runs/$tag.stats.csv"
      cp results/current/producer.log "results/runs/$tag.producer.log"
      gzip -c results/current/consumed.csv > "results/runs/$tag.consumed.csv.gz"
      gzip -c results/current/produced.csv > "results/runs/$tag.produced.csv.gz"
    done
  done
done

docker compose down -v >/dev/null 2>&1 || true
python3 scripts/summarise.py > results/summary.md
cat results/summary.md
