package broker

import (
	"context"

	nsq "github.com/nsqio/go-nsq"
)

// Durability: nsqd runs with --mem-queue-size=0 and --sync-every=1, so every
// message hits the diskqueue and is fsynced before nsqd replies OK.
type nsqProducer struct {
	p     *nsq.Producer
	topic string
}

func newNSQProducer(addr, topic string) (Producer, error) {
	cfg := nsq.NewConfig()
	p, err := nsq.NewProducer(addr, cfg)
	if err != nil {
		return nil, err
	}
	p.SetLoggerLevel(nsq.LogLevelError)
	return &nsqProducer{p: p, topic: topic}, nil
}

func (p *nsqProducer) Publish(_ context.Context, msg []byte) error {
	return p.p.Publish(p.topic, msg)
}
func (p *nsqProducer) Close() error { p.p.Stop(); return nil }

type nsqConsumer struct {
	c    *nsq.Consumer
	addr string
}

func newNSQConsumer(addr, topic, channel string) (Consumer, error) {
	cfg := nsq.NewConfig()
	cfg.MaxInFlight = 256
	c, err := nsq.NewConsumer(topic, channel, cfg)
	if err != nil {
		return nil, err
	}
	c.SetLoggerLevel(nsq.LogLevelError)
	return &nsqConsumer{c: c, addr: addr}, nil
}

func (c *nsqConsumer) Run(ctx context.Context, fn func([]byte)) error {
	c.c.AddHandler(nsq.HandlerFunc(func(m *nsq.Message) error {
		fn(m.Body)
		return nil // go-nsq FINs on nil
	}))
	if err := c.c.ConnectToNSQD(c.addr); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
func (c *nsqConsumer) Close() error { c.c.Stop(); return nil }
