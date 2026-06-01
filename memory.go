package gjobs

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

// EnqueueDedup mirrors SQLiteStorage.EnqueueDedup. See that method for the
// behavioral contract.
func (m *MemoryStorage) EnqueueDedup(_ context.Context, job *Job, mode DedupMode) (EnqueueResult, error) {
	if job.DedupKey == "" {
		cp := *job
		m.mu.Lock()
		m.jobs[job.ID] = &cp
		m.mu.Unlock()
		return EnqueueResult{Action: EnqueueInserted}, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()

	// 1. Drop completed-with-expired-TTL entries for this key.
	for id, j := range m.jobs {
		if j.DedupKey != job.DedupKey {
			continue
		}
		if j.Status != StatusDone && j.Status != StatusFailed {
			continue
		}
		if j.DedupKeyExpiresAt == nil || !j.DedupKeyExpiresAt.After(now) {
			delete(m.jobs, id)
		}
	}

	// 2. Find a blocking conflict. Prefer running > pending > completed-with-TTL.
	var existing *Job
	rank := func(j *Job) int {
		switch j.Status {
		case StatusRunning:
			return 0
		case StatusPending:
			return 1
		default:
			return 2
		}
	}
	for _, j := range m.jobs {
		if j.DedupKey != job.DedupKey {
			continue
		}
		if existing == nil || rank(j) < rank(existing) {
			existing = j
		}
	}

	if existing == nil {
		cp := *job
		m.jobs[job.ID] = &cp
		return EnqueueResult{Action: EnqueueInserted}, nil
	}

	result := EnqueueResult{ExistingJobID: existing.ID, ExistingStatus: existing.Status}

	switch mode {
	case DedupModeReplace:
		if existing.Status == StatusRunning {
			result.Action = EnqueueSkippedRunning
			return result, nil
		}
		delete(m.jobs, existing.ID)
		cp := *job
		m.jobs[job.ID] = &cp
		result.Action = EnqueueReplaced
		return result, nil
	default: // DedupModeIgnore
		result.Action = EnqueueSkippedDuplicate
		return result, nil
	}
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
		return fmt.Errorf("gjobs: job %q not found", id)
	}
	now := time.Now()
	j.Status = StatusDone
	j.UpdatedAt = now
	if j.DedupTTL > 0 {
		t := now.Add(j.DedupTTL)
		j.DedupKeyExpiresAt = &t
	}
	return nil
}

func (m *MemoryStorage) MarkFailed(_ context.Context, id string, errMsg string, retryAt *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return fmt.Errorf("gjobs: job %q not found", id)
	}
	now := time.Now()
	j.Attempts++
	j.LastError = errMsg
	j.UpdatedAt = now
	if retryAt != nil {
		j.Status = StatusPending
		j.RunAt = *retryAt
		return nil
	}
	j.Status = StatusFailed
	if j.DedupTTL > 0 {
		t := now.Add(j.DedupTTL)
		j.DedupKeyExpiresAt = &t
	}
	return nil
}

// RecoverStuck resets any in-memory jobs stuck in 'running' back to 'pending'.
func (m *MemoryStorage) RecoverStuck(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, j := range m.jobs {
		if j.Status == StatusRunning {
			j.Status = StatusPending
			j.UpdatedAt = now
		}
	}
	return nil
}

func (m *MemoryStorage) MarkPending(_ context.Context, id string, runAt time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok {
		return fmt.Errorf("gjobs: job %q not found", id)
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
		return fmt.Errorf("gjobs: cron %q not found", name)
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
		return fmt.Errorf("gjobs: job %q not found", id)
	}
	if j.Status != StatusFailed {
		return fmt.Errorf("gjobs: job %q is not failed (status=%s)", id, j.Status)
	}
	now := time.Now()
	j.Status = StatusPending
	j.RunAt = now
	j.Attempts = 0
	j.LastError = ""
	j.UpdatedAt = now
	return nil
}
