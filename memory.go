package jobs

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"
)

// MemoryStorage is a non-persistent in-memory Storage implementation.
// Jobs are lost on process exit. Designed for testing and local development.
type MemoryStorage struct {
	mu    sync.Mutex
	jobs  map[string]*Job
	crons map[string]*CronEntry
}

// NewMemoryStorage returns a ready-to-use in-memory storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		jobs:  make(map[string]*Job),
		crons: make(map[string]*CronEntry),
	}
}

func (m *MemoryStorage) Enqueue(_ context.Context, job *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *job
	m.jobs[job.ID] = &cp
	return nil
}

// Claim atomically marks up to limit due pending jobs as running.
func (m *MemoryStorage) Claim(_ context.Context, limit int) ([]*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	var due []*Job
	for _, j := range m.jobs {
		if j.Status == StatusPending && !j.RunAt.After(now) {
			due = append(due, j)
		}
	}
	slices.SortFunc(due, func(a, b *Job) int {
		return a.RunAt.Compare(b.RunAt)
	})
	if len(due) > limit {
		due = due[:limit]
	}

	out := make([]*Job, 0, len(due))
	for _, j := range due {
		j.Status = StatusRunning
		j.UpdatedAt = now
		cp := *j
		out = append(out, &cp)
	}
	return out, nil
}

func (m *MemoryStorage) MarkDone(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return fmt.Errorf("jobs: job %q not found", id)
	}
	j.Status = StatusDone
	j.UpdatedAt = time.Now()
	return nil
}

func (m *MemoryStorage) MarkFailed(_ context.Context, id string, errMsg string, retryAt *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return fmt.Errorf("jobs: job %q not found", id)
	}
	j.Attempts++
	j.LastError = errMsg
	j.UpdatedAt = time.Now()
	if retryAt != nil {
		j.Status = StatusPending
		j.RunAt = *retryAt
	} else {
		j.Status = StatusFailed
	}
	return nil
}

func (m *MemoryStorage) MarkPending(_ context.Context, id string, runAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return fmt.Errorf("jobs: job %q not found", id)
	}
	j.Status = StatusPending
	j.RunAt = runAt
	j.UpdatedAt = time.Now()
	return nil
}

func (m *MemoryStorage) UpsertCron(_ context.Context, c *CronEntry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *c
	m.crons[c.Name] = &cp
	return nil
}

func (m *MemoryStorage) DueCrons(_ context.Context) ([]*CronEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	var due []*CronEntry
	for _, c := range m.crons {
		if !c.NextRun.After(now) {
			cp := *c
			due = append(due, &cp)
		}
	}
	return due, nil
}

func (m *MemoryStorage) UpdateCronRun(_ context.Context, name string, last, next time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.crons[name]
	if !ok {
		return fmt.Errorf("jobs: cron %q not found", name)
	}
	c.LastRun = &last
	c.NextRun = next
	return nil
}

func (m *MemoryStorage) Close() error { return nil }

// ── DashboardStorage ──────────────────────────────────────────────────────────

func (m *MemoryStorage) Stats(_ context.Context) (JobStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var st JobStats
	for _, j := range m.jobs {
		switch j.Status {
		case StatusPending:
			st.Pending++
		case StatusRunning:
			st.Running++
		case StatusDone:
			st.Done++
		case StatusFailed:
			st.Failed++
		}
	}
	return st, nil
}

func (m *MemoryStorage) Jobs(_ context.Context, status Status, limit, offset int) ([]*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var all []*Job
	for _, j := range m.jobs {
		if status == "" || j.Status == status {
			cp := *j
			all = append(all, &cp)
		}
	}
	slices.SortFunc(all, func(a, b *Job) int {
		return b.UpdatedAt.Compare(a.UpdatedAt) // DESC
	})

	if offset >= len(all) {
		return nil, nil
	}
	all = all[offset:]
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func (m *MemoryStorage) RetryJob(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return fmt.Errorf("jobs: job %q not found", id)
	}
	if j.Status != StatusFailed {
		return fmt.Errorf("jobs: job %q is not failed (status=%s)", id, j.Status)
	}
	now := time.Now()
	j.Status = StatusPending
	j.RunAt = now
	j.Attempts = 0
	j.LastError = ""
	j.UpdatedAt = now
	return nil
}
