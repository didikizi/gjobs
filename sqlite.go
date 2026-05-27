package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
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
    last_error  TEXT
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

// SQLiteStorage implements Storage using a local SQLite file.
// Use NewSQLiteStorage to create an instance.
type SQLiteStorage struct {
	db *sql.DB
}

// NewSQLiteStorage opens (or creates) the SQLite database at path and
// applies the schema. A path of ":memory:" is valid for testing.
func NewSQLiteStorage(path string) (*SQLiteStorage, error) {
	dsn := fmt.Sprintf(
		"file:%s?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on",
		path,
	)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("jobs: open db: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite WAL allows one writer
	if _, err = db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("jobs: apply schema: %w", err)
	}
	return &SQLiteStorage{db: db}, nil
}

// Enqueue inserts a new job into the pending queue.
func (s *SQLiteStorage) Enqueue(ctx context.Context, job *Job) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO jobs (id, type, payload, status, attempts, max_retries, run_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.ID, job.Type, job.Payload, string(job.Status),
		job.Attempts, job.MaxRetries,
		job.RunAt.UTC(), job.CreatedAt.UTC(), job.UpdatedAt.UTC(),
	)
	return err
}

// Claim atomically marks up to limit pending jobs as running and returns them.
func (s *SQLiteStorage) Claim(ctx context.Context, limit int) ([]*Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	now := time.Now().UTC()
	rows, err := tx.QueryContext(ctx, `
		UPDATE jobs
		SET status = 'running', updated_at = ?
		WHERE id IN (
			SELECT id FROM jobs
			WHERE status = 'pending' AND run_at <= ?
			ORDER BY run_at ASC
			LIMIT ?
		)
		RETURNING id, type, payload, status, attempts, max_retries,
		          run_at, created_at, updated_at, last_error`,
		now, now, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs, err := scanJobs(rows)
	if err != nil {
		return nil, err
	}
	return jobs, tx.Commit()
}

// MarkDone sets job status to done.
func (s *SQLiteStorage) MarkDone(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET status='done', updated_at=? WHERE id=?`,
		time.Now().UTC(), id,
	)
	return err
}

// MarkFailed sets job status to failed. If retryAt is non-nil, the job is
// rescheduled as pending at that time; otherwise it stays failed (dead-letter).
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
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs
		SET status='failed', attempts=attempts+1, last_error=?, updated_at=?
		WHERE id=?`,
		errMsg, time.Now().UTC(), id,
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
	const cols = `id, type, payload, status, attempts, max_retries, run_at, created_at, updated_at, last_error`
	var (
		rows *sql.Rows
		err  error
	)
	if status == "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT `+cols+` FROM jobs ORDER BY updated_at DESC LIMIT ? OFFSET ?`,
			limit, offset)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT `+cols+` FROM jobs WHERE status=? ORDER BY updated_at DESC LIMIT ? OFFSET ?`,
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

// scanJobs reads rows into a []*Job slice.
func scanJobs(rows *sql.Rows) ([]*Job, error) {
	var out []*Job
	for rows.Next() {
		j := &Job{}
		var status string
		var lastError sql.NullString
		if err := rows.Scan(
			&j.ID, &j.Type, &j.Payload, &status,
			&j.Attempts, &j.MaxRetries,
			&j.RunAt, &j.CreatedAt, &j.UpdatedAt, &lastError,
		); err != nil {
			return nil, err
		}
		j.Status = Status(status)
		j.LastError = lastError.String
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
