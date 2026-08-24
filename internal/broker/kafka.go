package broker

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// Durability: acks=all against a single-node KRaft cluster whose topic has
// replication factor 1 and min.insync.replicas=1 (see docker-compose.yml and
// the ADR). acks=all therefore means "fsync path of one leader", not replicated.
type kafkaProducer struct{ w *kafka.Writer }

func newKafkaProducer(addr, topic string) (Producer, error) {
	return &kafkaProducer{w: &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 5 * time.Millisecond,
	}}, nil
}

func (p *kafkaProducer) Publish(ctx context.Context, msg []byte) error {
	return p.w.WriteMessages(ctx, kafka.Message{Value: msg})
}
func (p *kafkaProducer) Close() error { return p.w.Close() }

type kafkaConsumer struct{ r *kafka.Reader }

// CommitInterval 1s is kafka-go's usual production setting: offsets lag the
// processed messages, and that lag is exactly the duplicate burst after a kill.
func newKafkaConsumer(addr, topic, group string) (Consumer, error) {
	return &kafkaConsumer{r: kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{addr},
		Topic:          topic,
		GroupID:        group,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})}, nil
}

func (c *kafkaConsumer) Run(ctx context.Context, fn func([]byte)) error {
	for {
		m, err := c.r.ReadMessage(ctx)
		if err != nil {
			return err
		}
		fn(m.Value)
	}
}
func (c *kafkaConsumer) Close() error { return c.r.Close() }
