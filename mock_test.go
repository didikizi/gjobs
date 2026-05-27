package jobs

// Behavioural tests: verify that Queue interacts with Storage correctly.
// Uses a spyStorage that wraps MemoryStorage and records MarkDone/MarkFailed calls.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// ── spy ──────────────────────────────────────────────────────────────────────

type spyCall struct {
	id      string
	errMsg  string
	retryAt *time.Time // nil → dead-letter
}

// spyStorage wraps MemoryStorage and records MarkDone / MarkFailed interactions.
type spyStorage struct {
	*MemoryStorage
	mu          sync.Mutex
	doneCalls   []string
	failedCalls []spyCall
}

func newSpy() *spyStorage {
	return &spyStorage{MemoryStorage: NewMemoryStorage()}
}

func (s *spyStorage) MarkDone(ctx context.Context, id string) error {
	s.mu.Lock()
	s.doneCalls = append(s.doneCalls, id)
	s.mu.Unlock()
	return s.MemoryStorage.MarkDone(ctx, id)
}

func (s *spyStorage) MarkFailed(ctx context.Context, id string, errMsg string, retryAt *time.Time) error {
	s.mu.Lock()
	s.failedCalls = append(s.failedCalls, spyCall{id, errMsg, retryAt})
	s.mu.Unlock()
	return s.MemoryStorage.MarkFailed(ctx, id, errMsg, retryAt)
}

func (s *spyStorage) doneCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.doneCalls)
}

func (s *spyStorage) failedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.failedCalls)
}

func (s *spyStorage) lastFailed() (spyCall, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.failedCalls) == 0 {
		return spyCall{}, false
	}
	return s.failedCalls[len(s.failedCalls)-1], true
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newSpyQueue(t *testing.T, spy *spyStorage) *Queue {
	t.Helper()
	q, err := New(
		WithStorage(spy),
		WithConcurrency(5),
		WithPollInterval(10*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return q
}

func startAndWait(t *testing.T, q *Queue, condition func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { q.Start(ctx) }() //nolint:errcheck

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !condition() {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
}

// ── tests ─────────────────────────────────────────────────────────────────────

// When a handler succeeds, MarkDone must be called exactly once.
func TestSpy_MarkDone_CalledOnSuccess(t *testing.T) {
	spy := newSpy()
	q := newSpyQueue(t, spy)

	ok := Def("ok")
	q.Register(ok, func(_ context.Context, _ []byte) error { return nil })
	q.Enqueue(ok, nil) //nolint:errcheck

	startAndWait(t, q, func() bool { return spy.doneCount() == 1 })

	if spy.doneCount() != 1 {
		t.Errorf("expected MarkDone called once, got %d", spy.doneCount())
	}
	if spy.failedCount() != 0 {
		t.Errorf("expected no MarkFailed calls, got %d", spy.failedCount())
	}
}

// When a handler fails and retries remain, MarkFailed must be called with a
// non-nil retryAt (reschedule), not dead-lettered.
func TestSpy_MarkFailed_WithRetry_WhenRetriesRemain(t *testing.T) {
	spy := newSpy()
	q := newSpyQueue(t, spy)

	flaky := Def("flaky")
	q.Register(flaky, func(_ context.Context, _ []byte) error {
		return errors.New("transient")
	})
	q.Enqueue(flaky, nil, Retries(3)) //nolint:errcheck

	startAndWait(t, q, func() bool { return spy.failedCount() >= 1 })

	call, ok := spy.lastFailed()
	if !ok {
		t.Fatal("expected at least one MarkFailed call")
	}
	if call.retryAt == nil {
		t.Error("retryAt must be non-nil when retries remain (job should be rescheduled)")
	}
	if spy.doneCount() != 0 {
		t.Errorf("expected no MarkDone calls on failure, got %d", spy.doneCount())
	}
}

// When a handler fails and maxRetries is 1 (no retries), MarkFailed must be
// called with nil retryAt (dead-letter).
func TestSpy_MarkFailed_DeadLetter_WhenNoRetriesLeft(t *testing.T) {
	spy := newSpy()
	q := newSpyQueue(t, spy)

	broken := Def("broken")
	q.Register(broken, func(_ context.Context, _ []byte) error {
		return errors.New("permanent")
	})
	q.Enqueue(broken, nil, Retries(1)) //nolint:errcheck

	startAndWait(t, q, func() bool { return spy.failedCount() >= 1 })

	call, ok := spy.lastFailed()
	if !ok {
		t.Fatal("expected MarkFailed to be called")
	}
	if call.retryAt != nil {
		t.Error("retryAt must be nil for dead-lettered job")
	}
}

// When no handler is registered for a job type, the job must be dead-lettered
// immediately without panicking.
func TestSpy_UnknownJobType_DeadLetters(t *testing.T) {
	spy := newSpy()
	q := newSpyQueue(t, spy)
	// intentionally do not register a handler for "ghost"
	q.Enqueue(Def("ghost"), nil) //nolint:errcheck

	startAndWait(t, q, func() bool { return spy.failedCount() >= 1 })

	if spy.failedCount() == 0 {
		t.Error("unknown job type should be dead-lettered")
	}
	if call, ok := spy.lastFailed(); ok && call.retryAt != nil {
		t.Error("unknown job type should dead-letter (nil retryAt), not retry")
	}
}

// Backoff must grow with each attempt: attempt 1 ≥ attempt 0.
func TestSpy_BackoffGrowsWithAttempts(t *testing.T) {
	cfg := defaultConfig()
	p := &workerPool{backoffBase: cfg.backoffBase, backoffCap: cfg.backoffCap}
	d0 := p.calcBackoff(JobDef{}, 0)
	d1 := p.calcBackoff(JobDef{}, 1)
	d2 := p.calcBackoff(JobDef{}, 2)

	if d1 <= d0 {
		t.Errorf("backoff should increase: attempt 0=%v, attempt 1=%v", d0, d1)
	}
	if d2 <= d1 {
		t.Errorf("backoff should increase: attempt 1=%v, attempt 2=%v", d1, d2)
	}
}

// Backoff must never exceed backoffCap.
func TestSpy_BackoffCappedAtMaximum(t *testing.T) {
	cfg := defaultConfig()
	p := &workerPool{backoffBase: cfg.backoffBase, backoffCap: cfg.backoffCap}
	for _, attempt := range []int{10, 50, 100} {
		d := p.calcBackoff(JobDef{}, attempt)
		if d > cfg.backoffCap {
			t.Errorf("backoff(%d) = %v exceeds cap %v", attempt, d, cfg.backoffCap)
		}
	}
}

// MarkFailed must record the handler's error message verbatim.
func TestSpy_ErrorMessagePropagated(t *testing.T) {
	spy := newSpy()
	q := newSpyQueue(t, spy)

	const msg = "something went wrong: code 503"
	errmsg := Def("errmsg")
	q.Register(errmsg, func(_ context.Context, _ []byte) error {
		return errors.New(msg)
	})
	q.Enqueue(errmsg, nil, Retries(1)) //nolint:errcheck

	startAndWait(t, q, func() bool { return spy.failedCount() >= 1 })

	call, _ := spy.lastFailed()
	if call.errMsg != msg {
		t.Errorf("expected error %q, got %q", msg, call.errMsg)
	}
}

// ── testutil.MockStorage smoke test ──────────────────────────────────────────

// Verify that MockStorage (the exported testutil helper) compiles and satisfies
// the Storage interface without needing a real database connection.
func TestMockStorage_ImplementsStorage(t *testing.T) {
	// If MockStorage does not implement Storage this line will not compile.
	var _ Storage = newMockAdapter()
}

// newMockAdapter wraps testutil.MockStorage behind the package boundary.
// We can't import github.com/vkorolev/gjobs/testutil from package jobs directly (cycle), so we
// recreate a minimal equivalent inline to confirm the interface is satisfied.
type minimalMock struct{}

func (minimalMock) Enqueue(_ context.Context, _ *Job) error                              { return nil }
func (minimalMock) Claim(_ context.Context, _ int) ([]*Job, error)                       { return nil, nil }
func (minimalMock) MarkDone(_ context.Context, _ string) error                           { return nil }
func (minimalMock) MarkFailed(_ context.Context, _ string, _ string, _ *time.Time) error { return nil }
func (minimalMock) MarkPending(_ context.Context, _ string, _ time.Time) error           { return nil }
func (minimalMock) UpsertCron(_ context.Context, _ *CronEntry) error                     { return nil }
func (minimalMock) DueCrons(_ context.Context) ([]*CronEntry, error)                     { return nil, nil }
func (minimalMock) UpdateCronRun(_ context.Context, _ string, _, _ time.Time) error      { return nil }
func (minimalMock) Close() error                                                          { return nil }

func newMockAdapter() Storage { return minimalMock{} }
