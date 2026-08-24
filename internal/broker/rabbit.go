package broker

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Durability: durable queue + persistent delivery mode + publisher confirms,
// and the publish call blocks until the broker confirms.
type rabbitProducer struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string
}

func declare(ch *amqp.Channel, queue string) error {
	_, err := ch.QueueDeclare(queue, true, false, false, false, nil)
	return err
}

func newRabbitProducer(addr, queue string) (Producer, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := declare(ch, queue); err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		return nil, err
	}
	return &rabbitProducer{conn: conn, ch: ch, queue: queue}, nil
}

func (p *rabbitProducer) Publish(ctx context.Context, msg []byte) error {
	c, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, "", p.queue, false, false,
		amqp.Publishing{DeliveryMode: amqp.Persistent, Body: msg})
	if err != nil {
		return err
	}
	_, err = c.WaitContext(ctx)
	return err
}

func (p *rabbitProducer) Close() error { _ = p.ch.Close(); return p.conn.Close() }

type rabbitConsumer struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string
}

func newRabbitConsumer(addr, queue string) (Consumer, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := declare(ch, queue); err != nil {
		return nil, err
	}
	if err := ch.Qos(256, 0, false); err != nil { // prefetch = in-flight window
		return nil, err
	}
	return &rabbitConsumer{conn: conn, ch: ch, queue: queue}, nil
}

func (c *rabbitConsumer) Run(ctx context.Context, fn func([]byte)) error {
	d, err := c.ch.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m, ok := <-d:
			if !ok {
				return nil
			}
			fn(m.Body)
			_ = m.Ack(false)
		}
	}
}
func (c *rabbitConsumer) Close() error { _ = c.ch.Close(); return c.conn.Close() }
