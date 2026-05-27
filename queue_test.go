package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// newTestQueue returns a Queue backed by an in-memory SQLite.
func newTestQueue(t *testing.T, opts ...Option) *Queue {
	t.Helper()
	s, err := NewSQLiteStorage(":memory:")
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	opts = append([]Option{
		WithStorage(s),
		WithConcurrency(5),
		WithPollInterval(20 * time.Millisecond),
	}, opts...)
	q, err := New(opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return q
}

// runQueue starts q in the background and returns a cancel func.
func runQueue(t *testing.T, q *Queue) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { q.Start(ctx) }() //nolint:errcheck
	return cancel
}

func TestJobExecution(t *testing.T) {
	q := newTestQueue(t)

	def := Def("ping")
	var called atomic.Bool
	q.Register(def, func(_ context.Context, _ []byte) error {
		called.Store(true)
		return nil
	})

	if err := q.Enqueue(def, nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	stop := runQueue(t, q)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !called.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if !called.Load() {
		t.Error("job was never executed")
	}
}

func TestTypedHandler(t *testing.T) {
	type Email struct {
		To string
	}

	q := newTestQueue(t)

	def := Def("email")
	var got Email
	var done atomic.Bool
	HandleDef[Email](q, def, func(_ context.Context, e Email) error {
		got = e
		done.Store(true)
		return nil
	})

	if err := q.Enqueue(def, Email{To: "test@example.com"}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	stop := runQueue(t, q)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !done.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if !done.Load() {
		t.Fatal("typed handler never called")
	}
	if got.To != "test@example.com" {
		t.Errorf("expected To=test@example.com, got %q", got.To)
	}
}

func TestRetryOnFailure(t *testing.T) {
	q := newTestQueue(t)

	def := Def("flaky")
	var attempts atomic.Int32
	q.Register(def, func(_ context.Context, _ []byte) error {
		attempts.Add(1)
		return errors.New("transient error")
	})

	if err := q.Enqueue(def, nil, Retries(3)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	stop := runQueue(t, q)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && attempts.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if attempts.Load() == 0 {
		t.Error("flaky job never attempted")
	}
}

func TestDelayedJob(t *testing.T) {
	q := newTestQueue(t)

	def := Def("delayed")
	var called atomic.Bool
	q.Register(def, func(_ context.Context, _ []byte) error {
		called.Store(true)
		return nil
	})

	if err := q.Enqueue(def, nil, After(200*time.Millisecond)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	stop := runQueue(t, q)
	defer stop()

	// Should not run immediately.
	time.Sleep(50 * time.Millisecond)
	if called.Load() {
		t.Error("delayed job ran too early")
	}

	// Should run after delay.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && !called.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if !called.Load() {
		t.Error("delayed job never ran")
	}
}

func TestDeadLetter(t *testing.T) {
	q := newTestQueue(t)

	def := Def("broken")
	var attempts atomic.Int32
	q.Register(def, func(_ context.Context, _ []byte) error {
		attempts.Add(1)
		return errors.New("always fails")
	})

	// maxRetries=1 → after first failure it should dead-letter immediately.
	if err := q.Enqueue(def, nil, Retries(1)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	stop := runQueue(t, q)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && attempts.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}

	// Verify it ended up as failed (not re-queued) by checking storage directly.
	s := q.storage.(*SQLiteStorage)
	rows, _ := s.db.QueryContext(context.Background(),
		`SELECT status FROM jobs WHERE type='broken'`)
	defer rows.Close()
	var status string
	rows.Next()
	rows.Scan(&status)
	if status != "failed" {
		t.Errorf("expected status=failed, got %q", status)
	}
}

func TestCronJob(t *testing.T) {
	q := newTestQueue(t)

	var fired atomic.Int32
	heartbeat := Def("heartbeat")
	if err := q.Schedule(heartbeat, "100ms", func(_ context.Context) error {
		fired.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	stop := runQueue(t, q)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && fired.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if fired.Load() == 0 {
		t.Error("cron job never fired")
	}
}

func TestGracefulShutdown(t *testing.T) {
	q := newTestQueue(t)

	running := make(chan struct{})
	finished := make(chan struct{})

	def := Def("slow")
	q.Register(def, func(_ context.Context, _ []byte) error {
		close(running)
		time.Sleep(200 * time.Millisecond)
		close(finished)
		return nil
	})

	if err := q.Enqueue(def, nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- q.Start(ctx) }()

	<-running // wait until job starts
	cancel()  // trigger shutdown

	select {
	case <-finished:
		// job completed before Stop returned — good
	case <-time.After(2 * time.Second):
		t.Error("graceful shutdown did not wait for in-flight job")
	}
	<-done
}

func TestPayloadRoundTrip(t *testing.T) {
	type Order struct {
		ID    int
		Items []string
	}

	q := newTestQueue(t)

	want := Order{ID: 42, Items: []string{"apple", "banana"}}
	var got Order
	var done atomic.Bool

	def := Def("order")
	q.Register(def, func(_ context.Context, raw []byte) error {
		json.Unmarshal(raw, &got) //nolint:errcheck
		done.Store(true)
		return nil
	})

	q.Enqueue(def, want) //nolint:errcheck

	stop := runQueue(t, q)
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !done.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if got.ID != want.ID || len(got.Items) != len(want.Items) {
		t.Errorf("payload mismatch: got %+v, want %+v", got, want)
	}
}

func TestCancelAll(t *testing.T) {
	q := newTestQueue(t)

	def := Def("cancellable")
	started := make(chan struct{})
	cancelled := make(chan struct{})

	q.Register(def, func(ctx context.Context, _ []byte) error {
		close(started)
		<-ctx.Done() // block until cancelled
		close(cancelled)
		return ctx.Err()
	})
	_ = q.Enqueue(def, nil)

	stop := runQueue(t, q)
	defer stop()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("job never started")
	}

	n := q.CancelAll(def)
	if n != 1 {
		t.Errorf("expected CancelAll to return 1, got %d", n)
	}

	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Error("handler context was not cancelled after CancelAll")
	}
}
