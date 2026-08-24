// Command analyze turns the producer and consumer logs into the run's metrics.
// Lost and duplicate are set differences over sequence ids, never estimates.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
)

type event struct {
	seq uint64
	ns  int64
}

func readCSV(path string, offset int64) []event {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	var out []event
	var seq uint64
	var ns int64
	for _, line := range splitLines(b) {
		if _, err := fmt.Sscanf(line, "%d,%d", &seq, &ns); err != nil {
			continue
		}
		out = append(out, event{seq, ns - offset})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ns < out[j].ns })
	return out
}

func splitLines(b []byte) []string {
	var out []string
	start := 0
	for i, c := range b {
		if c == '\n' {
			if i > start {
				out = append(out, string(b[start:i]))
			}
			start = i + 1
		}
	}
	return out
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p / 100 * float64(len(sorted)-1))
	return sorted[i]
}

// Report is the per-run result written to results/ as JSON.
type Report struct {
	Broker             string  `json:"broker"`
	Arm                string  `json:"arm"`
	Run                int     `json:"run"`
	Published          int     `json:"published"`
	Deliveries         int     `json:"deliveries"`
	Unique             int     `json:"unique"`
	Lost               int     `json:"lost"`
	Duplicates         int     `json:"duplicates"`
	DupsAfterRestartMs int     `json:"duplicates_after_restart"`
	ThroughputPerSec   float64 `json:"throughput_msgs_per_sec"`
	P50ms              float64 `json:"latency_p50_ms"`
	P95ms              float64 `json:"latency_p95_ms"`
	P99ms              float64 `json:"latency_p99_ms"`
	MaxMs              float64 `json:"latency_max_ms"`
	BacklogPeak        int     `json:"backlog_peak"`
	RedeliveryDelayMs  float64 `json:"redelivery_delay_ms"`
	CatchUpMs          float64 `json:"catch_up_ms"`
}

func main() {
	produced := flag.String("produced", "", "producer csv")
	consumed := flag.String("consumed", "", "consumer csv")
	offset := flag.Int64("offset-ns", 0, "container clock minus host clock, ns")
	restart := flag.Int64("restart-ns", 0, "host time the consumer was restarted")
	brokerName := flag.String("broker", "", "broker name")
	arm := flag.String("arm", "", "control|crash")
	run := flag.Int("run", 0, "run index")
	flag.Parse()

	pub := readCSV(*produced, 0)
	con := readCSV(*consumed, *offset)

	pubAt := make(map[uint64]int64, len(pub))
	for _, e := range pub {
		pubAt[e.seq] = e.ns
	}
	first := make(map[uint64]int64, len(pub))
	var lat []float64
	dups, dupsAfter := 0, 0
	var firstAfterRestart int64
	for _, e := range con {
		if _, seen := first[e.seq]; seen {
			dups++
			if *restart > 0 && e.ns >= *restart {
				dupsAfter++
			}
			continue
		}
		first[e.seq] = e.ns
		if p, ok := pubAt[e.seq]; ok {
			lat = append(lat, float64(e.ns-p)/1e6)
		}
		if *restart > 0 && e.ns >= *restart && firstAfterRestart == 0 {
			firstAfterRestart = e.ns
		}
	}
	sort.Float64s(lat)

	// Backlog over time: published minus uniquely consumed, event by event.
	type te struct {
		ns   int64
		kind int
	}
	tl := make([]te, 0, len(pub)+len(first))
	for _, e := range pub {
		tl = append(tl, te{e.ns, +1})
	}
	for _, ns := range first {
		tl = append(tl, te{ns, -1})
	}
	sort.Slice(tl, func(i, j int) bool { return tl[i].ns < tl[j].ns })
	backlog, peak := 0, 0
	for _, e := range tl {
		backlog += e.kind
		if backlog > peak {
			peak = backlog
		}
	}

	var span float64
	if len(con) > 0 {
		span = float64(con[len(con)-1].ns-con[0].ns) / 1e9
	}
	r := Report{
		Broker: *brokerName, Arm: *arm, Run: *run,
		Published: len(pub), Deliveries: len(con), Unique: len(first),
		Lost: len(pub) - len(first), Duplicates: dups, DupsAfterRestartMs: dupsAfter,
		P50ms: pct(lat, 50), P95ms: pct(lat, 95), P99ms: pct(lat, 99), MaxMs: pct(lat, 100),
		BacklogPeak: peak,
	}
	if span > 0 {
		r.ThroughputPerSec = float64(len(first)) / span
	}
	if *restart > 0 {
		if firstAfterRestart > 0 {
			r.RedeliveryDelayMs = float64(firstAfterRestart-*restart) / 1e6
		}
		// Caught up = last first-delivery in the run, relative to restart.
		var last int64
		for _, ns := range first {
			if ns > last {
				last = ns
			}
		}
		r.CatchUpMs = float64(last-*restart) / 1e6
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(b))
}
