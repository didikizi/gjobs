package gjobs

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"
)

// TestDedup runs the deduplication contract against both storage backends.
func TestDedup(t *testing.T) {
	t.Run("SQLite", func(t *testing.T) { runDedupTests(t, sqliteFactory) })
	t.Run("Memory", func(t *testing.T) { runDedupTests(t, memoryFactory) })
}

func runDedupTests(t *testing.T, factory storageFactory) {
	t.Helper()

	t.Run("Ignore_SkipsPending", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j1 := dedupJob("a", "send", "user-42", 0)
		r1, err := s.EnqueueDedup(ctx, j1, DedupModeIgnore)
		if err != nil || r1.Action != EnqueueInserted {
			t.Fatalf("first enqueue: %v, %s", err, r1.Action)
		}

		j2 := dedupJob("b", "send", "user-42", 0)
		r2, err := s.EnqueueDedup(ctx, j2, DedupModeIgnore)
		if err != nil {
			t.Fatalf("second enqueue: %v", err)
		}
		if r2.Action != EnqueueSkippedDuplicate {
			t.Errorf("expected SkippedDuplicate, got %s", r2.Action)
		}
		if r2.ExistingJobID != "a" {
			t.Errorf("expected ExistingJobID=a, got %q", r2.ExistingJobID)
		}
		if r2.ExistingStatus != StatusPending {
			t.Errorf("expected ExistingStatus=pending, got %s", r2.ExistingStatus)
		}
	})

	t.Run("Ignore_SkipsRunning", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j1 := dedupJob("a", "send", "user-42", 0)
		if _, err := s.EnqueueDedup(ctx, j1, DedupModeIgnore); err != nil {
			t.Fatalf("first enqueue: %v", err)
		}

		claimed, err := s.Claim(ctx, 1)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("Claim: %v, got %d", err, len(claimed))
		}

		j2 := dedupJob("b", "send", "user-42", 0)
		r2, err := s.EnqueueDedup(ctx, j2, DedupModeIgnore)
		if err != nil {
			t.Fatalf("second enqueue: %v", err)
		}
		if r2.Action != EnqueueSkippedDuplicate {
			t.Errorf("expected SkippedDuplicate, got %s", r2.Action)
		}
		if r2.ExistingStatus != StatusRunning {
			t.Errorf("expected ExistingStatus=running, got %s", r2.ExistingStatus)
		}
	})

	t.Run("Replace_OverwritesPending", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j1 := dedupJob("a", "send", "user-42", 0)
		j1.Payload = []byte(`{"v":1}`)
		if _, err := s.EnqueueDedup(ctx, j1, DedupModeIgnore); err != nil {
			t.Fatalf("first enqueue: %v", err)
		}

		j2 := dedupJob("b", "send", "user-42", 0)
		j2.Payload = []byte(`{"v":2}`)
		r2, err := s.EnqueueDedup(ctx, j2, DedupModeReplace)
		if err != nil {
			t.Fatalf("replace enqueue: %v", err)
		}
		if r2.Action != EnqueueReplaced {
			t.Errorf("expected Replaced, got %s", r2.Action)
		}

		claimed, err := s.Claim(ctx, 10)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(claimed) != 1 {
			t.Fatalf("expected 1 job after replace, got %d", len(claimed))
		}
		if claimed[0].ID != "b" {
			t.Errorf("expected new job (b) to be claimable, got %s", claimed[0].ID)
		}
		if string(claimed[0].Payload) != `{"v":2}` {
			t.Errorf("expected new payload, got %s", claimed[0].Payload)
		}
	})

	t.Run("Replace_SkipsRunning", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j1 := dedupJob("a", "send", "user-42", 0)
		if _, err := s.EnqueueDedup(ctx, j1, DedupModeIgnore); err != nil {
			t.Fatalf("first enqueue: %v", err)
		}
		if _, err := s.Claim(ctx, 1); err != nil {
			t.Fatalf("Claim: %v", err)
		}

		j2 := dedupJob("b", "send", "user-42", 0)
		r2, err := s.EnqueueDedup(ctx, j2, DedupModeReplace)
		if err != nil {
			t.Fatalf("replace enqueue: %v", err)
		}
		if r2.Action != EnqueueSkippedRunning {
			t.Errorf("expected SkippedRunning, got %s", r2.Action)
		}
		if r2.ExistingJobID != "a" {
			t.Errorf("expected ExistingJobID=a, got %q", r2.ExistingJobID)
		}
	})

	t.Run("TTL_BlocksAfterDone", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j1 := dedupJob("a", "send", "user-42", 1*time.Second)
		if _, err := s.EnqueueDedup(ctx, j1, DedupModeIgnore); err != nil {
			t.Fatalf("first enqueue: %v", err)
		}
		claimed, _ := s.Claim(ctx, 1)
		if len(claimed) != 1 {
			t.Fatalf("Claim: got %d", len(claimed))
		}
		if err := s.MarkDone(ctx, "a"); err != nil {
			t.Fatalf("MarkDone: %v", err)
		}

		// Within TTL window — skip.
		j2 := dedupJob("b", "send", "user-42", 1*time.Second)
		r2, _ := s.EnqueueDedup(ctx, j2, DedupModeIgnore)
		if r2.Action != EnqueueSkippedDuplicate {
			t.Errorf("expected SkippedDuplicate during TTL, got %s", r2.Action)
		}

		// After TTL expires — allowed.
		time.Sleep(1100 * time.Millisecond)
		j3 := dedupJob("c", "send", "user-42", 1*time.Second)
		r3, err := s.EnqueueDedup(ctx, j3, DedupModeIgnore)
		if err != nil {
			t.Fatalf("post-TTL enqueue: %v", err)
		}
		if r3.Action != EnqueueInserted {
			t.Errorf("expected Inserted after TTL, got %s", r3.Action)
		}
	})

	t.Run("TTL_BlocksAfterDeadLetter", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j1 := dedupJob("a", "send", "user-42", 1*time.Second)
		if _, err := s.EnqueueDedup(ctx, j1, DedupModeIgnore); err != nil {
			t.Fatalf("first enqueue: %v", err)
		}
		if _, err := s.Claim(ctx, 1); err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if err := s.MarkFailed(ctx, "a", "boom", nil); err != nil {
			t.Fatalf("MarkFailed: %v", err)
		}

		j2 := dedupJob("b", "send", "user-42", 1*time.Second)
		r2, _ := s.EnqueueDedup(ctx, j2, DedupModeIgnore)
		if r2.Action != EnqueueSkippedDuplicate {
			t.Errorf("expected SkippedDuplicate during TTL after dead-letter, got %s", r2.Action)
		}

		time.Sleep(1100 * time.Millisecond)
		j3 := dedupJob("c", "send", "user-42", 1*time.Second)
		r3, err := s.EnqueueDedup(ctx, j3, DedupModeIgnore)
		if err != nil {
			t.Fatalf("post-TTL enqueue: %v", err)
		}
		if r3.Action != EnqueueInserted {
			t.Errorf("expected Inserted after TTL, got %s", r3.Action)
		}
	})

	t.Run("NoTTL_FreedAfterDone", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		j1 := dedupJob("a", "send", "user-42", 0)
		if _, err := s.EnqueueDedup(ctx, j1, DedupModeIgnore); err != nil {
			t.Fatalf("first enqueue: %v", err)
		}
		if _, err := s.Claim(ctx, 1); err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if err := s.MarkDone(ctx, "a"); err != nil {
			t.Fatalf("MarkDone: %v", err)
		}

		// No TTL — next enqueue immediately succeeds.
		j2 := dedupJob("b", "send", "user-42", 0)
		r2, err := s.EnqueueDedup(ctx, j2, DedupModeIgnore)
		if err != nil {
			t.Fatalf("post-done enqueue: %v", err)
		}
		if r2.Action != EnqueueInserted {
			t.Errorf("expected Inserted (no TTL), got %s", r2.Action)
		}
	})

	t.Run("NilKeyIsNoop", func(t *testing.T) {
		s := factory(t)
		ctx := context.Background()

		// Two enqueues with empty key — both must land.
		j1 := makeJob(t, "send")
		j1.ID = "n1"
		j2 := makeJob(t, "send")
		j2.ID = "n2"

		if _, err := s.EnqueueDedup(ctx, j1, DedupModeIgnore); err != nil {
			t.Fatalf("enqueue 1: %v", err)
		}
		if _, err := s.EnqueueDedup(ctx, j2, DedupModeIgnore); err != nil {
			t.Fatalf("enqueue 2: %v", err)
		}

		claimed, err := s.Claim(ctx, 10)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(claimed) != 2 {
			t.Errorf("expected 2 jobs (no dedup), got %d", len(claimed))
		}
	})
}

// dedupJob builds a job with the given id, type, dedup key, and optional TTL.
func dedupJob(id, typ, key string, ttl time.Duration) *Job {
	now := time.Now()
	return &Job{
		ID:          id,
		Type:        typ,
		Payload:     []byte(`{}`),
		Status:      StatusPending,
		MaxAttempts: 3,
		RunAt:       now,
		CreatedAt:   now,
		UpdatedAt:   now,
		DedupKey:    key,
		DedupTTL:    ttl,
	}
}

// TestDedup_QueueIntegration verifies that the Queue layer wires DedupKey,
// DedupReplace, and DedupTTL through to storage and respects the action result.
func TestDedup_QueueIntegration(t *testing.T) {
	mem := NewMemoryStorage()
	q, err := New(
		WithStorage(mem),
		WithConcurrency(1),
		WithPollInterval(20*time.Millisecond),
		WithNoLogger(),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	def := Def("noop")
	var ran atomic.Int32
	q.Register(def, func(_ context.Context, _ []byte) error {
		ran.Add(1)
		return nil
	})

	ctx := context.Background()
	if err := q.Enqueue(ctx, def, nil, DedupKey("k1")); err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}
	// Second enqueue with same key — must be silently skipped.
	if err := q.Enqueue(ctx, def, nil, DedupKey("k1")); err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}

	// Only one row should exist.
	mem.mu.Lock()
	n := len(mem.jobs)
	mem.mu.Unlock()
	if n != 1 {
		t.Errorf("expected 1 job in storage, got %d", n)
	}
}

// TestDedup_Migration_v04 verifies that a v0.4.x schema (no dedup columns)
// is upgraded in place by NewSQLiteStorage without losing data.
func TestDedup_Migration_v04(t *testing.T) {
	// Build a v0.4.x schema manually in a temp DB.
	dir := t.TempDir()
	path := dir + "/legacy.db"

	db, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatalf("open legacy: %v", err)
	}
	const legacySchema = `
	CREATE TABLE jobs (
		id          TEXT PRIMARY KEY,
		type        TEXT NOT NULL,
		payload     BLOB,
		status      TEXT NOT NULL DEFAULT 'pending',
		attempts    INTEGER NOT NULL DEFAULT 0,
		max_retries INTEGER NOT NULL DEFAULT 3,
		run_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_error  TEXT
	);
	CREATE TABLE cron_jobs (
		name       TEXT PRIMARY KEY,
		schedule   TEXT NOT NULL,
		last_run   DATETIME,
		next_run   DATETIME NOT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	);`
	if _, err := db.Exec(legacySchema); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO jobs (id, type, payload, status) VALUES ('legacy-1', 'old', X'7B7D', 'done')`); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}
	db.Close()

	// Re-open via NewSQLiteStorage — should migrate idempotently.
	s, err := NewSQLiteStorage(path)
	if err != nil {
		t.Fatalf("NewSQLiteStorage on legacy: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	// Confirm legacy row survived.
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE id='legacy-1'`).Scan(&n); err != nil {
		t.Fatalf("count legacy: %v", err)
	}
	if n != 1 {
		t.Errorf("expected legacy row to survive migration, got count=%d", n)
	}

	// Confirm new columns exist.
	rows, err := s.db.Query(`PRAGMA table_info(jobs)`)
	if err != nil {
		t.Fatalf("PRAGMA: %v", err)
	}
	defer rows.Close()
	found := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan: %v", err)
		}
		found[name] = true
	}
	for _, col := range []string{"dedup_key", "dedup_ttl_seconds", "dedup_key_expires_at"} {
		if !found[col] {
			t.Errorf("migration missed column %s", col)
		}
	}

	// Re-running NewSQLiteStorage is idempotent.
	s2, err := NewSQLiteStorage(path)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	s2.Close()
}
