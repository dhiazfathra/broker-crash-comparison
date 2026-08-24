package broker

import (
	"testing"
	"time"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	body := Body(42)
	if len(body) != PayloadSize-16 {
		t.Fatalf("body size %d", len(body))
	}
	if string(Body(42)) != string(body) {
		t.Fatal("seeded body is not deterministic")
	}
	now := time.Now()
	m := Encode(7, now, body)
	if len(m) != PayloadSize {
		t.Fatalf("wire size %d", len(m))
	}
	seq, ns, err := Decode(m)
	if err != nil || seq != 7 || ns != now.UnixNano() {
		t.Fatalf("roundtrip: %d %d %v", seq, ns, err)
	}
	if _, _, err := Decode([]byte{1, 2}); err == nil {
		t.Fatal("short message should error")
	}
}

func TestUnknownBroker(t *testing.T) {
	if _, err := NewProducer("redis", "x", "y"); err == nil {
		t.Fatal("expected error")
	}
	if _, err := NewConsumer("redis", "x", "y", "z"); err == nil {
		t.Fatal("expected error")
	}
}
