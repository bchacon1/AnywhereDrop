package memory

import (
	"context"
	"testing"
	"time"

	"anywheredrop/backend/internal/bus"
)

func TestSubscribeBeforePublishDelivers(t *testing.T) {
	b := New()
	ctx := context.Background()
	s1, _ := b.Subscribe(ctx, "R")
	s2, _ := b.Subscribe(ctx, "R")
	defer s1.Close()
	defer s2.Close()

	_ = b.Publish(ctx, "R", bus.Envelope{FromPeer: "a", Kind: bus.KindRelay})
	for _, s := range []bus.Subscription{s1, s2} {
		select {
		case e := <-s.Messages():
			if e.FromPeer != "a" || e.Room != "R" {
				t.Fatalf("got %+v", e)
			}
		case <-time.After(time.Second):
			t.Fatal("no delivery")
		}
	}
}

func TestPublishBeforeSubscribeIsLost(t *testing.T) {
	b := New()
	ctx := context.Background()
	_ = b.Publish(ctx, "R", bus.Envelope{Kind: bus.KindPeerJoined})
	s, _ := b.Subscribe(ctx, "R")
	defer s.Close()
	select {
	case e := <-s.Messages():
		t.Fatalf("unexpected delivery %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestOrderPreservedPerPublisher(t *testing.T) {
	b := New()
	ctx := context.Background()
	s, _ := b.Subscribe(ctx, "R")
	defer s.Close()
	for i := 0; i < 10; i++ {
		_ = b.Publish(ctx, "R", bus.Envelope{FromGen: i})
	}
	for i := 0; i < 10; i++ {
		e := <-s.Messages()
		if e.FromGen != i {
			t.Fatalf("order: got %d want %d", e.FromGen, i)
		}
	}
}

func TestCloseIsIdempotentAndStopsDelivery(t *testing.T) {
	b := New()
	ctx := context.Background()
	s, _ := b.Subscribe(ctx, "R")
	s.Close()
	s.Close()
	_ = b.Publish(ctx, "R", bus.Envelope{})
	if _, ok := <-s.Messages(); ok {
		t.Fatal("expected closed channel")
	}
}

// Publish and Close racing must never panic (send on closed channel).
func TestPublishAndCloseRace(t *testing.T) {
	b := New()
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		s, _ := b.Subscribe(ctx, "R")
		done := make(chan struct{})
		go func() {
			for j := 0; j < 20; j++ {
				_ = b.Publish(ctx, "R", bus.Envelope{FromGen: j})
			}
			close(done)
		}()
		s.Close()
		<-done
	}
}
