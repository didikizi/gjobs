package jobs

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// storageFactory creates a fresh Storage for each sub-test.
type storageFactory func(t *testing.T) Storage

// runStorageTests runs the full storage contract suite against any backend.
// Every sub-test receives an isolated storage from factory.
func runStorageTests(t *testing.T, factory storageFactory) {
	t.Helper()

	t.Run("EnqueueAndClaim", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j := makeJob(t, "email")
		if err := s.Enqueue(ctx, j); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}

		claimed, err := s.Claim(ctx, 10)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(claimed) != 1 {
			t.Fatalf("expected 1 claimed job, got %d", len(claimed))
		}
		if claimed[0].Status != StatusRunning {
			t.Errorf("expected status running, got %s", claimed[0].Status)
		}

		// Second claim returns nothing — already running.
		again, _ := s.Claim(ctx, 10)
		if len(again) != 0 {
			t.Errorf("expected 0 jobs on second claim, got %d", len(again))
		}
	})

	t.Run("MarkDone", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j := makeJob(t, "resize")
		s.Enqueue(ctx, j)   //nolint:errcheck
		claimed, _ := s.Claim(ctx, 1)

		if err := s.MarkDone(ctx, claimed[0].ID); err != nil {
			t.Fatalf("MarkDone: %v", err)
		}
		more, _ := s.Claim(ctx, 10)
		if len(more) != 0 {
			t.Errorf("done job should not be reclaimable")
		}
	})

	t.Run("MarkFailed_Retry", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j := makeJob(t, "webhook")
		s.Enqueue(ctx, j) //nolint:errcheck
		claimed, _ := s.Claim(ctx, 1)

		retryAt := time.Now().Add(-1 * time.Millisecond) // immediate
		if err := s.MarkFailed(ctx, claimed[0].ID, "timeout", &retryAt); err != nil {
			t.Fatalf("MarkFailed: %v", err)
		}

		retried, err := s.Claim(ctx, 10)
		if err != nil {
			t.Fatalf("Claim after retry: %v", err)
		}
		if len(retried) != 1 {
			t.Fatalf("expected 1 retried job, got %d", len(retried))
		}
		if retried[0].Attempts != 1 {
			t.Errorf("expected attempts=1, got %d", retried[0].Attempts)
		}
	})

	t.Run("MarkFailed_DeadLetter", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j := makeJob(t, "broken")
		s.Enqueue(ctx, j) //nolint:errcheck
		claimed, _ := s.Claim(ctx, 1)

		if err := s.MarkFailed(ctx, claimed[0].ID, "permanent", nil); err != nil {
			t.Fatalf("MarkFailed dead-letter: %v", err)
		}
		again, _ := s.Claim(ctx, 10)
		if len(again) != 0 {
			t.Errorf("dead-letter job should not be reclaimable")
		}
	})

	t.Run("DelayedJob", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j := makeJob(t, "delayed")
		j.RunAt = time.Now().Add(1 * time.Hour)
		s.Enqueue(ctx, j) //nolint:errcheck

		claimed, _ := s.Claim(ctx, 10)
		if len(claimed) != 0 {
			t.Errorf("future job should not be claimed yet, got %d", len(claimed))
		}
	})

	t.Run("CronUpsertAndDue", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		entry := &CronEntry{
			Name:      "cleanup",
			Schedule:  "1h",
			NextRun:   time.Now().Add(-1 * time.Second),
			CreatedAt: time.Now(),
		}
		if err := s.UpsertCron(ctx, entry); err != nil {
			t.Fatalf("UpsertCron: %v", err)
		}

		due, err := s.DueCrons(ctx)
		if err != nil {
			t.Fatalf("DueCrons: %v", err)
		}
		if len(due) != 1 || due[0].Name != "cleanup" {
			t.Errorf("expected 1 due cron, got %v", due)
		}

		now := time.Now()
		if err := s.UpdateCronRun(ctx, "cleanup", now, now.Add(time.Hour)); err != nil {
			t.Fatalf("UpdateCronRun: %v", err)
		}
		due2, _ := s.DueCrons(ctx)
		if len(due2) != 0 {
			t.Errorf("cron should not be due after update, got %d", len(due2))
		}
	})

	t.Run("ConcurrentClaim", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		for i := range 20 {
			j := makeJob(t, fmt.Sprintf("task-%d", i))
			s.Enqueue(ctx, j) //nolint:errcheck
		}

		const workers = 5
		results := make(chan int, workers)
		for range workers {
			go func() {
				claimed, _ := s.Claim(ctx, 5)
				results <- len(claimed)
			}()
		}
		total := 0
		for range workers {
			total += <-results
		}
		if total != 20 {
			t.Errorf("expected 20 claimed across workers, got %d", total)
		}
	})
}

// makeJob creates a test job with a unique ID derived from the test name.
func makeJob(t *testing.T, jobType string) *Job {
	t.Helper()
	now := time.Now()
	return &Job{
		ID:         t.Name() + "-" + jobType,
		Type:       jobType,
		Payload:    []byte(`{"key":"val"}`),
		Status:     StatusPending,
		Attempts:   0,
		MaxRetries: 3,
		RunAt:      now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// newJob is kept for backward-compatibility with queue_test.go and bench_test.go.
func newJob(jobType string) *Job {
	now := time.Now()
	return &Job{
		ID:         "test-" + jobType,
		Type:       jobType,
		Payload:    []byte(`{"key":"val"}`),
		Status:     StatusPending,
		Attempts:   0,
		MaxRetries: 3,
		RunAt:      now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

// ── SQLite backend ────────────────────────────────────────────────────────────

func sqliteFactory(t *testing.T) Storage {
	t.Helper()
	s, err := NewSQLiteStorage(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStorage: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// newTestStorage is kept for backward-compatibility with queue_test.go.
func newTestStorage(t *testing.T) *SQLiteStorage {
	t.Helper()
	s, err := NewSQLiteStorage(":memory:")
	if err != nil {
		t.Fatalf("NewSQLiteStorage: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSQLiteStorage(t *testing.T) {
	runStorageTests(t, sqliteFactory)
}
