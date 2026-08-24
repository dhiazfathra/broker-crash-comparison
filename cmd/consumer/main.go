// Command consumer runs inside a container so it can be SIGKILLed mid-stream.
// It appends "seq,receiveNanos" per delivery, unbuffered, so a kill loses no
// record of work already done.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/dhiazfathra/broker-crash-comparison/internal/broker"
)

func main() {
	name := flag.String("broker", "kafka", "kafka|nsq|rabbit")
	addr := flag.String("addr", "", "broker address")
	topic := flag.String("topic", "bench", "topic/queue name")
	group := flag.String("group", "bench", "consumer group/channel")
	out := flag.String("out", "consumed.csv", "append-only csv")
	clock := flag.Bool("clock", false, "print this container's UnixNano and exit")
	flag.Parse()

	if *clock {
		fmt.Println(time.Now().UnixNano())
		return
	}

	f, err := os.OpenFile(*out, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	c, err := broker.NewConsumer(*name, *addr, *topic, *group)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	buf := make([]byte, 0, 64)
	err = c.Run(ctx, func(msg []byte) {
		seq, _, derr := broker.Decode(msg)
		if derr != nil {
			return
		}
		buf = buf[:0]
		buf = append(buf, fmt.Sprintf("%d,%d\n", seq, time.Now().UnixNano())...)
		_, _ = f.Write(buf)
	})
	_ = c.Close()
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}
