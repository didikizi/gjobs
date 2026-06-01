package gjobs

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    payload     BLOB,
    status      TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    run_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error  TEXT,
    dedup_key            TEXT,
    dedup_ttl_seconds    INTEGER,
    dedup_key_expires_at DATETIME
);
CREATE INDEX IF NOT EXISTS idx_jobs_poll ON jobs(status, run_at);

CREATE TABLE IF NOT EXISTS cron_jobs (
    name       TEXT PRIMARY KEY,
    schedule   TEXT NOT NULL,
    last_run   DATETIME,
    next_run   DATETIME NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`

// dedupActiveIndex enforces at most one (pending|running) job per dedup_key.
// Completed jobs with a TTL are kept out of the index; their lock is enforced
// at write time by EnqueueDedup (which deletes expired entries first and
// SELECTs for a TTL-active row before inserting).
const dedupActiveIndex = `
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_dedup_active
ON jobs(dedup_key)
WHERE dedup_key IS NOT NULL AND status IN ('pending', 'running');
`

// SQLiteStorage implements Storage using a local SQLite file.
// Use NewSQLiteStorage to create an instance.
type SQLiteStorage struct {
	db *sql.DB
}

// NewSQLiteStorage opens (or creates) the SQLite database at path and
// applies the schema. A path of ":memory:" is valid for testing.
//
// Migrations: ALTER TABLE ADD COLUMN is run idempotently via PRAGMA table_info
// for v0.4.x databases that lack the dedup_* columns. No data is rewritten.
func NewSQLiteStorage(path string) (*SQLiteStorage, error) {
	dsn := fmt.Sprintf(
		"file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)",
		path,
	)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("gjobs: open db: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite WAL allows one writer
	s := &SQLiteStorage{db: db}
	if err := s.initSchema(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *SQLiteStorage) initSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("gjobs: apply schema: %w", err)
	}
	// Upgrade path for v0.4.x databases: jobs table exists without dedup columns.
	for _, col := range []struct{ name, typ string }{
		{"dedup_key", "TEXT"},
		{"dedup_ttl_seconds", "INTEGER"},
		{"dedup_key_expires_at", "DATETIME"},
	} {
		if err := s.addColumnIfMissing(ctx, "jobs", col.name, col.typ); err != nil {
			return fmt.Errorf("gjobs: migrate column %s: %w", col.name, err)
		}
	}
	if _, err := s.db.ExecContext(ctx, dedupActiveIndex); err != nil {
		return fmt.Errorf("gjobs: create dedup index: %w", err)
	}
	return nil
}

func (s *SQLiteStorage) addColumnIfMissing(ctx context.Context, table, col, typ string) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == col {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, col, typ))
	return err
}

// Enqueue inserts a new job into the pending queue. Dedup fields on job are
// persisted as-is. For deduplication semantics (uniqueness check, replace),
// use EnqueueDedup.
func (s *SQLiteStorage) Enqueue(ctx context.Context, job *Job) error {
	_, err := s.db.ExecContext(ctx, insertJobSQL,
		insertJobArgs(job)...,
	)
	return err
}

const insertJobSQL = `
INSERT INTO jobs (id, type, payload, status, attempts, max_retries, run_at, created_at, updated_at,
                  dedup_key, dedup_ttl_seconds, dedup_key_expires_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

func insertJobArgs(job *Job) []any {
	return []any{
		job.ID, job.Type, job.Payload, string(job.Status),
		job.Attempts, job.MaxAttempts,
		job.RunAt.UTC(), job.CreatedAt.UTC(), job.UpdatedAt.UTC(),
		nullStr(job.DedupKey), nullSeconds(job.DedupTTL), nullTime(job.DedupKeyExpiresAt),
	}
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullSeconds(d time.Duration) any {
	if d <= 0 {
		return nil
	}
	// Round up so sub-second TTLs are not silently truncated to zero.
	secs := int64((d + time.Second - 1) / time.Second)
	return secs
}

func nullTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

// EnqueueDedup inserts a job with deduplication semantics.
//   - DedupModeIgnore: on conflict, skip and return EnqueueSkippedDuplicate
//   - DedupModeReplace: on non-running conflict, atomically delete + insert
//     and return EnqueueReplaced; on running conflict return EnqueueSkippedRunning
//
// If job.DedupKey is empty, the call falls through to plain Enqueue (returning
// EnqueueInserted).
func (s *SQLiteStorage) EnqueueDedup(ctx context.Context, job *Job, mode DedupMode) (EnqueueResult, error) {
	if job.DedupKey == "" {
		return EnqueueResult{Action: EnqueueInserted}, s.Enqueue(ctx, job)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return EnqueueResult{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now().UTC()

	// 1. Drop completed-with-expired-TTL entries for this key so they no longer block.
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM jobs
		WHERE dedup_key = ?
		  AND status IN ('done', 'failed')
		  AND (dedup_key_expires_at IS NULL OR dedup_key_expires_at <= ?)`,
		job.DedupKey, now,
	); err != nil {
		return EnqueueResult{}, err
	}

	// 2. Find a blocking conflict, preferring active over completed-with-TTL.
	var existingID, existingStatus string
	err = tx.QueryRowContext(ctx, `
		SELECT id, status FROM jobs
		WHERE dedup_key = ?
		ORDER BY
		  CASE status
		    WHEN 'running' THEN 0
		    WHEN 'pending' THEN 1
		    ELSE 2
		  END
		LIMIT 1`,
		job.DedupKey,
	).Scan(&existingID, &existingStatus)
	if err != nil && err != sql.ErrNoRows {
		return EnqueueResult{}, err
	}

	if err == sql.ErrNoRows {
		if _, err := tx.ExecContext(ctx, insertJobSQL, insertJobArgs(job)...); err != nil {
			return EnqueueResult{}, err
		}
		return EnqueueResult{Action: EnqueueInserted}, tx.Commit()
	}

	result := EnqueueResult{ExistingJobID: existingID, ExistingStatus: Status(existingStatus)}

	switch mode {
	case DedupModeReplace:
		if Status(existingStatus) == StatusRunning {
			result.Action = EnqueueSkippedRunning
			return result, tx.Commit()
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM jobs WHERE id=?`, existingID); err != nil {
			return EnqueueResult{}, err
		}
		if _, err := tx.ExecContext(ctx, insertJobSQL, insertJobArgs(job)...); err != nil {
			return EnqueueResult{}, err
		}
		result.Action = EnqueueReplaced
		return result, tx.Commit()
	default: // DedupModeIgnore
		result.Action = EnqueueSkippedDuplicate
		return result, tx.Commit()
	}
}

// Claim atomically marks up to limit pending jobs as running and returns them.
// UPDATE...RETURNING is atomic in SQLite so no explicit transaction is needed.
func (s *SQLiteStorage) Claim(ctx context.Context, limit int) ([]*Job, error) {
	now := time.Now().UTC()
	rows, err := s.db.QueryContext(ctx, `
		UPDATE jobs
		SET status = 'running', updated_at = ?
		WHERE id IN (
			SELECT id FROM jobs
			WHERE status = 'pending' AND run_at <= ?
			ORDER BY run_at ASC
			LIMIT ?
		)
		RETURNING `+jobCols,
		now, now, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

// MarkDone sets job status to done. If the job has a positive dedup_ttl_seconds,
// dedup_key_expires_at is set to now + ttl.
func (s *SQLiteStorage) MarkDone(ctx context.Context, id string) error {
	return s.markCompleted(ctx, id, StatusDone, "", nil)
}

// MarkFailed sets job status to failed. If retryAt is non-nil, the job is
// rescheduled as pending at that time and dedup_key_expires_at is left alone.
// Otherwise it stays failed (dead-letter); the TTL window starts if configured.
func (s *SQLiteStorage) MarkFailed(ctx context.Context, id string, errMsg string, retryAt *time.Time) error {
	if retryAt != nil {
		_, err := s.db.ExecContext(ctx, `
			UPDATE jobs
			SET status='pending', attempts=attempts+1, last_error=?, run_at=?, updated_at=?
			WHERE id=?`,
			errMsg, retryAt.UTC(), time.Now().UTC(), id,
		)
		return err
	}
	return s.markCompleted(ctx, id, StatusFailed, errMsg, nil)
}

// markCompleted is the shared write path for terminal status transitions
// (done / dead-letter). It computes dedup_key_expires_at from the row's
// dedup_ttl_seconds column.
func (s *SQLiteStorage) markCompleted(ctx context.Context, id string, status Status, errMsg string, _ *time.Time) error {
	now := time.Now().UTC()

	var ttlSecs sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		`SELECT dedup_ttl_seconds FROM jobs WHERE id=?`, id,
	).Scan(&ttlSecs)
	if err == sql.ErrNoRows {
		return nil // match prior MarkDone semantics: missing row is a no-op
	}
	if err != nil {
		return err
	}

	var expiresAt any
	if ttlSecs.Valid && ttlSecs.Int64 > 0 {
		expiresAt = now.Add(time.Duration(ttlSecs.Int64) * time.Second)
	}

	if status == StatusFailed {
		_, err := s.db.ExecContext(ctx, `
			UPDATE jobs
			SET status='failed', attempts=attempts+1, last_error=?, updated_at=?, dedup_key_expires_at=?
			WHERE id=?`,
			errMsg, now, expiresAt, id,
		)
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE jobs SET status='done', updated_at=?, dedup_key_expires_at=? WHERE id=?`,
		now, expiresAt, id,
	)
	return err
}

// RecoverStuck resets jobs stuck in 'running' back to 'pending' after a crash.
func (s *SQLiteStorage) RecoverStuck(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status='pending', updated_at=? WHERE status='running'`,
		time.Now().UTC(),
	)
	return err
}

// MarkPending reschedules a job as pending at runAt (used internally by cron).
func (s *SQLiteStorage) MarkPending(ctx context.Context, id string, runAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status='pending', run_at=?, updated_at=? WHERE id=?`,
		runAt.UTC(), time.Now().UTC(), id,
	)
	return err
}

// UpsertCron creates or updates a cron entry.
func (s *SQLiteStorage) UpsertCron(ctx context.Context, c *CronEntry) error {
	var lastRun interface{}
	if c.LastRun != nil {
		lastRun = c.LastRun.UTC()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO cron_jobs (name, schedule, last_run, next_run, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE
		SET schedule=excluded.schedule, next_run=excluded.next_run`,
		c.Name, c.Schedule, lastRun, c.NextRun.UTC(), c.CreatedAt.UTC(),
	)
	return err
}

// DueCrons returns all cron entries whose next_run is in the past.
func (s *SQLiteStorage) DueCrons(ctx context.Context) ([]*CronEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT name, schedule, last_run, next_run, created_at
		FROM cron_jobs WHERE next_run <= ?`,
		time.Now().UTC(),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCrons(rows)
}

// UpdateCronRun sets last_run and next_run after a cron fires.
func (s *SQLiteStorage) UpdateCronRun(ctx context.Context, name string, last, next time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE cron_jobs SET last_run=?, next_run=? WHERE name=?`,
		last.UTC(), next.UTC(), name,
	)
	return err
}

// Close releases the database connection.
func (s *SQLiteStorage) Close() error {
	return s.db.Close()
}

// ── DashboardStorage ──────────────────────────────────────────────────────────

func (s *SQLiteStorage) Stats(ctx context.Context) (JobStats, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM jobs GROUP BY status`)
	if err != nil {
		return JobStats{}, err
	}
	defer rows.Close()
	var st JobStats
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return st, err
		}
		switch Status(status) {
		case StatusPending:
			st.Pending = n
		case StatusRunning:
			st.Running = n
		case StatusDone:
			st.Done = n
		case StatusFailed:
			st.Failed = n
		}
	}
	return st, rows.Err()
}

func (s *SQLiteStorage) Jobs(ctx context.Context, status Status, limit, offset int) ([]*Job, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if status == "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT `+jobCols+` FROM jobs ORDER BY updated_at DESC LIMIT ? OFFSET ?`,
			limit, offset)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT `+jobCols+` FROM jobs WHERE status=? ORDER BY updated_at DESC LIMIT ? OFFSET ?`,
			string(status), limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

func (s *SQLiteStorage) RetryJob(ctx context.Context, id string) error {
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs
		SET status='pending', run_at=?, attempts=0, last_error='', updated_at=?
		WHERE id=? AND status='failed'`,
		now, now, id,
	)
	return err
}

// jobCols is the canonical column list for SELECTs that feed scanJobs.
const jobCols = `id, type, payload, status, attempts, max_retries,
	run_at, created_at, updated_at, last_error,
	dedup_key, dedup_ttl_seconds, dedup_key_expires_at`

// scanJobs reads rows into a []*Job slice.
func scanJobs(rows *sql.Rows) ([]*Job, error) {
	var out []*Job
	for rows.Next() {
		j := &Job{}
		var status string
		var lastError sql.NullString
		var dedupKey sql.NullString
		var dedupTTL sql.NullInt64
		var dedupExp sql.NullTime
		if err := rows.Scan(
			&j.ID, &j.Type, &j.Payload, &status,
			&j.Attempts, &j.MaxAttempts,
			&j.RunAt, &j.CreatedAt, &j.UpdatedAt, &lastError,
			&dedupKey, &dedupTTL, &dedupExp,
		); err != nil {
			return nil, err
		}
		j.Status = Status(status)
		j.LastError = lastError.String
		j.DedupKey = dedupKey.String
		if dedupTTL.Valid && dedupTTL.Int64 > 0 {
			j.DedupTTL = time.Duration(dedupTTL.Int64) * time.Second
		}
		if dedupExp.Valid {
			t := dedupExp.Time
			j.DedupKeyExpiresAt = &t
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// scanCrons reads rows into a []*CronEntry slice.
func scanCrons(rows *sql.Rows) ([]*CronEntry, error) {
	var out []*CronEntry
	for rows.Next() {
		c := &CronEntry{}
		var lastRun sql.NullTime
		if err := rows.Scan(&c.Name, &c.Schedule, &lastRun, &c.NextRun, &c.CreatedAt); err != nil {
			return nil, err
		}
		if lastRun.Valid {
			t := lastRun.Time
			c.LastRun = &t
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
