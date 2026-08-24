// Package broker gives Kafka, NSQ and RabbitMQ one shape so the harness is not
// a confound: same producer loop, same consumer loop, same payload codec.
package broker

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"
)

// PayloadSize is the total wire size of every message, header included.
const PayloadSize = 512

// Header is 8 bytes of sequence id + 8 bytes of publish timestamp (unix nanos).
const headerSize = 16

// Body is the fixed-seed filler shared by every message of a run.
func Body(seed int64) []byte {
	b := make([]byte, PayloadSize-headerSize)
	//nolint:gosec // deterministic filler, not cryptography.
	r := rand.New(rand.NewSource(seed))
	r.Read(b)
	return b
}

// Encode writes seq + publish time in front of the fixed filler.
func Encode(seq uint64, publishedAt time.Time, body []byte) []byte {
	m := make([]byte, headerSize+len(body))
	binary.BigEndian.PutUint64(m[0:8], seq)
	binary.BigEndian.PutUint64(m[8:16], uint64(publishedAt.UnixNano()))
	copy(m[headerSize:], body)
	return m
}

// Decode reverses Encode. It errors on anything shorter than a header.
func Decode(m []byte) (seq uint64, publishedAtNanos int64, err error) {
	if len(m) < headerSize {
		return 0, 0, fmt.Errorf("short message: %d bytes", len(m))
	}
	return binary.BigEndian.Uint64(m[0:8]), int64(binary.BigEndian.Uint64(m[8:16])), nil
}

// Producer publishes durably: Publish must not return before the broker has
// acknowledged the write under the durability settings documented in the ADR.
type Producer interface {
	Publish(ctx context.Context, msg []byte) error
	Close() error
}

// Consumer delivers messages to fn. Redelivery on crash is the broker's job.
type Consumer interface {
	Run(ctx context.Context, fn func(msg []byte)) error
	Close() error
}

// NewProducer / NewConsumer dispatch on broker name: kafka | nsq | rabbit.
func NewProducer(name, addr, topic string) (Producer, error) {
	switch name {
	case "kafka":
		return newKafkaProducer(addr, topic)
	case "nsq":
		return newNSQProducer(addr, topic)
	case "rabbit":
		return newRabbitProducer(addr, topic)
	}
	return nil, fmt.Errorf("unknown broker %q", name)
}

func NewConsumer(name, addr, topic, group string) (Consumer, error) {
	switch name {
	case "kafka":
		return newKafkaConsumer(addr, topic, group)
	case "nsq":
		return newNSQConsumer(addr, topic, group)
	case "rabbit":
		return newRabbitConsumer(addr, topic)
	}
	return nil, fmt.Errorf("unknown broker %q", name)
}
