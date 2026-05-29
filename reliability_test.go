package jobs

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestRecoverStuck_Storage verifies RecoverStuck in the storage contract suite.
// A job claimed (running) without being completed must be re-claimable after RecoverStuck.
func TestRecoverStuck_Storage(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { runRecoverStuckTest(t, sqliteFactory) })
	t.Run("Memory", func(t *testing.T) { runRecoverStuckTest(t, memoryFactory) })
}

func runRecoverStuckTest(t *testing.T, factory storageFactory) {
	t.Helper()
	s := factory(t)
	ctx := context.Background()

	j := makeJob(t, "stuck")
	if err := s.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Claim the job — simulates a worker picking it up before a crash.
	claimed, err := s.Claim(ctx, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: %v, got %d", err, len(claimed))
	}
	if claimed[0].Status != StatusRunning {
		t.Fatalf("expected running, got %s", claimed[0].Status)
	}

	// Without marking done — simulate crash. RecoverStuck should reset it.
	if err := s.RecoverStuck(ctx); err != nil {
		t.Fatalf("RecoverStuck: %v", err)
	}

	again, err := s.Claim(ctx, 10)
	if err != nil {
		t.Fatalf("Claim after recovery: %v", err)
	}
	if len(again) != 1 {
		t.Errorf("expected 1 recovered job after RecoverStuck, got %d", len(again))
	}
}

// TestRecoverStuck_QueueRestart verifies that Start() calls RecoverStuck so that
// jobs stuck in 'running' (from a previous crash) are re-executed on restart.
func TestRecoverStuck_QueueRestart(t *testing.T) {
	mem := NewMemoryStorage()
	ctx := context.Background()

	def := Def("crashable")

	// Directly enqueue and claim to simulate a crashed in-flight job.
	j := &Job{
		ID: "crash-sim", Type: def.Name, Status: StatusPending,
		RunAt: time.Now(), CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := mem.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := mem.Claim(ctx, 1); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	// Now the job is 'running' with no worker holding it — crash simulated.

	// "Restart": new queue on the same storage. Start() must call RecoverStuck.
	q, err := New(
		WithStorage(mem),
		WithConcurrency(1),
		WithPollInterval(10*time.Millisecond),
		WithNoLogger(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var ran atomic.Bool
	q.Register(def, func(_ context.Context, _ []byte) error {
		ran.Store(true)
		return nil
	})

	qCtx, cancel := context.WithCancel(context.Background())
	go q.Start(qCtx) //nolint:errcheck

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !ran.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()

	if !ran.Load() {
		t.Error("stuck job was not recovered and re-executed after queue restart")
	}
}

// TestPanicRecovery verifies that a panicking handler does not kill the worker
// goroutine and that the job ends up dead-lettered with "panic:" in last_error.
func TestPanicRecovery(t *testing.T) {
	s := newTestStorage(t)
	q, err := New(
		WithStorage(s),
		WithConcurrency(1),
		WithPollInterval(10*time.Millisecond),
		WithNoLogger(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	def := Def("panicky")
	q.Register(def, func(_ context.Context, _ []byte) error {
		panic("boom")
	})

	// maxRetries=1 → one attempt, then dead-letter.
	if err := q.Enqueue(context.Background(), def, nil, Retries(1)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	stop := runQueue(t, q)
	defer stop()

	// Poll until the job reaches 'failed' status.
	var status, lastError string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
		row := s.db.QueryRowContext(context.Background(),
			`SELECT status, COALESCE(last_error,'') FROM jobs WHERE type='panicky'`)
		if err := row.Scan(&status, &lastError); err != nil {
			continue
		}
		if status == "failed" {
			break
		}
	}

	if status != "failed" {
		t.Errorf("expected panicking job to be dead-lettered (status=failed), got %q", status)
	}
	if !strings.Contains(lastError, "panic:") {
		t.Errorf("expected last_error to contain 'panic:', got %q", lastError)
	}
}

// TestBackoffValues verifies that calcBackoff returns the values documented in
// the README table: attempt 1→30s, 2→1m, 3→2m, 4→4m, 5→8m with base=30s.
func TestBackoffValues(t *testing.T) {
	p := &workerPool{backoffBase: 30 * time.Second, backoffCap: 1 * time.Hour}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 30 * time.Second},
		{2, 1 * time.Minute},
		{3, 2 * time.Minute},
		{4, 4 * time.Minute},
		{5, 8 * time.Minute},
	}

	for _, tc := range cases {
		got := p.calcBackoff(JobDef{}, tc.attempt)
		if got != tc.want {
			t.Errorf("calcBackoff(attempt=%d) = %v, want %v", tc.attempt, got, tc.want)
		}
	}
}

// TestRegisterAfterStartPanics verifies that calling Register after Start panics.
func TestRegisterAfterStartPanics(t *testing.T) {
	q, err := New(
		WithStorage(NewMemoryStorage()),
		WithConcurrency(1),
		WithPollInterval(10*time.Millisecond),
		WithNoLogger(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go q.Start(ctx) //nolint:errcheck

	// Give Start a moment to set started=true.
	time.Sleep(50 * time.Millisecond)
	cancel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected Register after Start to panic, but it did not")
		}
	}()
	q.Register(Def("late"), func(_ context.Context, _ []byte) error { return nil })
}
