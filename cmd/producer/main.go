// Command producer publishes at a fixed rate and logs "seq,publishNanos" per
// acknowledged message. It runs on the host; the consumer runs in a container.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/dhiazfathra/broker-crash-comparison/internal/broker"
)

func main() {
	name := flag.String("broker", "kafka", "kafka|nsq|rabbit")
	addr := flag.String("addr", "", "broker address")
	topic := flag.String("topic", "bench", "topic/queue name")
	rate := flag.Int("rate", 2000, "messages per second")
	dur := flag.Duration("duration", 30*time.Second, "run duration")
	workers := flag.Int("workers", 32, "concurrent publish slots")
	seed := flag.Int64("seed", 42, "payload seed")
	out := flag.String("out", "produced.csv", "output csv")
	flag.Parse()

	p, err := broker.NewProducer(*name, *addr, *topic)
	if err != nil {
		log.Fatal(err)
	}
	defer p.Close()

	f, err := os.Create(*out)
	if err != nil {
		log.Fatal(err)
	}
	w := bufio.NewWriterSize(f, 1<<20)
	defer func() { _ = w.Flush(); _ = f.Close() }()

	body := broker.Body(*seed)
	ctx, cancel := context.WithTimeout(context.Background(), *dur+30*time.Second)
	defer cancel()

	type rec struct {
		seq uint64
		ns  int64
	}
	jobs := make(chan uint64, *workers*4)
	recs := make(chan rec, 1<<16)
	var wg sync.WaitGroup
	var fails int
	var mu sync.Mutex

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := range jobs {
				ts := time.Now()
				if err := p.Publish(ctx, broker.Encode(seq, ts, body)); err != nil {
					mu.Lock()
					fails++
					mu.Unlock()
					continue
				}
				recs <- rec{seq, ts.UnixNano()}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		for r := range recs {
			fmt.Fprintf(w, "%d,%d\n", r.seq, r.ns)
		}
		close(done)
	}()

	start := time.Now()
	total := int(float64(*rate) * dur.Seconds())
	period := time.Second / time.Duration(*rate)
	var behind int
	for i := 0; i < total; i++ {
		target := start.Add(time.Duration(i) * period)
		if d := time.Until(target); d > 0 {
			time.Sleep(d)
		} else if d < -100*time.Millisecond {
			behind++
		}
		jobs <- uint64(i)
	}
	close(jobs)
	wg.Wait()
	close(recs)
	<-done
	elapsed := time.Since(start)
	fmt.Printf("produced=%d failed=%d elapsed=%.3fs offered_rate=%d achieved_rate=%.1f late_slots=%d\n",
		total-fails, fails, elapsed.Seconds(), *rate, float64(total-fails)/elapsed.Seconds(), behind)
}
