// Package testutil provides testing helpers for code that depends on jobs.Storage.
package testutil

import (
	"context"
	"sync"
	"time"

	"github.com/didikizi/gjobs"
)

// Call records a single invocation of a MockStorage method.
type Call struct {
	Method string
	Args   []any
}

// MockStorage is a configurable implementation of jobs.Storage.
// Set the *Fn fields before use to control return values.
// Every invocation is appended to Calls for post-hoc assertions.
//
// Unset Fn fields fall back to no-op defaults (nil error, empty results).
//
//	mock := testutil.NewMockStorage()
//	mock.ClaimFn = func(ctx context.Context, limit int) ([]*jobs.Job, error) {
//	    return []*jobs.Job{{ID: "1", Type: "email"}}, nil
//	}
//	q, _ := jobs.New(jobs.WithStorage(mock))
type MockStorage struct {
	EnqueueFn        func(ctx context.Context, job *jobs.Job) error
	ClaimFn          func(ctx context.Context, limit int) ([]*jobs.Job, error)
	MarkDoneFn       func(ctx context.Context, id string) error
	MarkFailedFn     func(ctx context.Context, id string, errMsg string, retryAt *time.Time) error
	MarkPendingFn    func(ctx context.Context, id string, runAt time.Time) error
	RecoverStuckFn   func(ctx context.Context) error
	UpsertCronFn     func(ctx context.Context, c *jobs.CronEntry) error
	DueCronsFn       func(ctx context.Context) ([]*jobs.CronEntry, error)
	UpdateCronRunFn  func(ctx context.Context, name string, last, next time.Time) error
	CloseFn          func() error

	mu    sync.Mutex
	Calls []Call
}

// NewMockStorage returns a MockStorage with all Fn fields unset (no-op defaults).
func NewMockStorage() *MockStorage {
	return &MockStorage{}
}

// CallsFor returns all recorded calls to the named method.
func (m *MockStorage) CallsFor(method string) []Call {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Call
	for _, c := range m.Calls {
		if c.Method == method {
			out = append(out, c)
		}
	}
	return out
}

func (m *MockStorage) record(method string, args ...any) {
	m.mu.Lock()
	m.Calls = append(m.Calls, Call{Method: method, Args: args})
	m.mu.Unlock()
}

func (m *MockStorage) Enqueue(ctx context.Context, job *jobs.Job) error {
	m.record("Enqueue", job)
	if m.EnqueueFn != nil {
		return m.EnqueueFn(ctx, job)
	}
	return nil
}

func (m *MockStorage) Claim(ctx context.Context, limit int) ([]*jobs.Job, error) {
	m.record("Claim", limit)
	if m.ClaimFn != nil {
		return m.ClaimFn(ctx, limit)
	}
	return nil, nil
}

func (m *MockStorage) MarkDone(ctx context.Context, id string) error {
	m.record("MarkDone", id)
	if m.MarkDoneFn != nil {
		return m.MarkDoneFn(ctx, id)
	}
	return nil
}

func (m *MockStorage) MarkFailed(ctx context.Context, id string, errMsg string, retryAt *time.Time) error {
	m.record("MarkFailed", id, errMsg, retryAt)
	if m.MarkFailedFn != nil {
		return m.MarkFailedFn(ctx, id, errMsg, retryAt)
	}
	return nil
}

func (m *MockStorage) MarkPending(ctx context.Context, id string, runAt time.Time) error {
	m.record("MarkPending", id, runAt)
	if m.MarkPendingFn != nil {
		return m.MarkPendingFn(ctx, id, runAt)
	}
	return nil
}

func (m *MockStorage) RecoverStuck(ctx context.Context) error {
	m.record("RecoverStuck")
	if m.RecoverStuckFn != nil {
		return m.RecoverStuckFn(ctx)
	}
	return nil
}

func (m *MockStorage) UpsertCron(ctx context.Context, c *jobs.CronEntry) error {
	m.record("UpsertCron", c)
	if m.UpsertCronFn != nil {
		return m.UpsertCronFn(ctx, c)
	}
	return nil
}

func (m *MockStorage) DueCrons(ctx context.Context) ([]*jobs.CronEntry, error) {
	m.record("DueCrons")
	if m.DueCronsFn != nil {
		return m.DueCronsFn(ctx)
	}
	return nil, nil
}

func (m *MockStorage) UpdateCronRun(ctx context.Context, name string, last, next time.Time) error {
	m.record("UpdateCronRun", name, last, next)
	if m.UpdateCronRunFn != nil {
		return m.UpdateCronRunFn(ctx, name, last, next)
	}
	return nil
}

func (m *MockStorage) Close() error {
	m.record("Close")
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}
