package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pgSchema = `
CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    payload     BYTEA,
    status      TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    run_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error  TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_poll ON jobs(status, run_at);

CREATE TABLE IF NOT EXISTS cron_jobs (
    name       TEXT PRIMARY KEY,
    schedule   TEXT NOT NULL,
    last_run   TIMESTAMPTZ,
    next_run   TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`

// PostgresStorage implements Storage using a PostgreSQL connection pool.
// Uses FOR UPDATE SKIP LOCKED for high-throughput concurrent job claiming.
type PostgresStorage struct {
	pool *pgxpool.Pool
}

// NewPostgresStorage connects to the given DSN and applies the schema.
//
//	s, err := jobs.NewPostgresStorage(ctx, "postgres://user:pass@localhost/mydb?sslmode=disable")
func NewPostgresStorage(ctx context.Context, connStr string) (*PostgresStorage, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("jobs: open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: ping postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, pgSchema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("jobs: apply schema: %w", err)
	}
	return &PostgresStorage{pool: pool}, nil
}

func (p *PostgresStorage) Enqueue(ctx context.Context, job *Job) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO jobs (id, type, payload, status, attempts, max_retries, run_at, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		job.ID, job.Type, job.Payload, string(job.Status),
		job.Attempts, job.MaxRetries,
		job.RunAt.UTC(), job.CreatedAt.UTC(), job.UpdatedAt.UTC(),
	)
	return err
}

// Claim atomically marks up to limit pending jobs as running.
// Uses FOR UPDATE SKIP LOCKED to allow multiple workers without contention.
func (p *PostgresStorage) Claim(ctx context.Context, limit int) ([]*Job, error) {
	rows, err := p.pool.Query(ctx, `
		UPDATE jobs
		SET status = 'running', updated_at = NOW()
		WHERE id IN (
			SELECT id FROM jobs
			WHERE status = 'pending' AND run_at <= NOW()
			ORDER BY run_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, type, payload, status, attempts, max_retries,
		          run_at, created_at, updated_at, last_error`,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgScanJobs(rows)
}

func (p *PostgresStorage) MarkDone(ctx context.Context, id string) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE jobs SET status='done', updated_at=NOW() WHERE id=$1`, id)
	return err
}

func (p *PostgresStorage) MarkFailed(ctx context.Context, id string, errMsg string, retryAt *time.Time) error {
	if retryAt != nil {
		_, err := p.pool.Exec(ctx, `
			UPDATE jobs
			SET status='pending', attempts=attempts+1, last_error=$1, run_at=$2, updated_at=NOW()
			WHERE id=$3`,
			errMsg, retryAt.UTC(), id,
		)
		return err
	}
	_, err := p.pool.Exec(ctx, `
		UPDATE jobs
		SET status='failed', attempts=attempts+1, last_error=$1, updated_at=NOW()
		WHERE id=$2`,
		errMsg, id,
	)
	return err
}

func (p *PostgresStorage) MarkPending(ctx context.Context, id string, runAt time.Time) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE jobs SET status='pending', run_at=$1, updated_at=NOW() WHERE id=$2`,
		runAt.UTC(), id,
	)
	return err
}

func (p *PostgresStorage) UpsertCron(ctx context.Context, c *CronEntry) error {
	var lastRun interface{}
	if c.LastRun != nil {
		lastRun = c.LastRun.UTC()
	}
	_, err := p.pool.Exec(ctx, `
		INSERT INTO cron_jobs (name, schedule, last_run, next_run, created_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (name) DO UPDATE
		SET schedule=EXCLUDED.schedule, next_run=EXCLUDED.next_run`,
		c.Name, c.Schedule, lastRun, c.NextRun.UTC(), c.CreatedAt.UTC(),
	)
	return err
}

func (p *PostgresStorage) DueCrons(ctx context.Context) ([]*CronEntry, error) {
	rows, err := p.pool.Query(ctx, `
		SELECT name, schedule, last_run, next_run, created_at
		FROM cron_jobs WHERE next_run <= NOW()`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgScanCrons(rows)
}

func (p *PostgresStorage) UpdateCronRun(ctx context.Context, name string, last, next time.Time) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE cron_jobs SET last_run=$1, next_run=$2 WHERE name=$3`,
		last.UTC(), next.UTC(), name,
	)
	return err
}

// Close releases all connections in the pool.
func (p *PostgresStorage) Close() error {
	p.pool.Close()
	return nil
}

// ── DashboardStorage ──────────────────────────────────────────────────────────

func (p *PostgresStorage) Stats(ctx context.Context) (JobStats, error) {
	rows, err := p.pool.Query(ctx, `SELECT status, COUNT(*) FROM jobs GROUP BY status`)
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

func (p *PostgresStorage) Jobs(ctx context.Context, status Status, limit, offset int) ([]*Job, error) {
	const cols = `id, type, payload, status, attempts, max_retries, run_at, created_at, updated_at, last_error`
	var (
		rows pgx.Rows
		err  error
	)
	if status == "" {
		rows, err = p.pool.Query(ctx,
			`SELECT `+cols+` FROM jobs ORDER BY updated_at DESC LIMIT $1 OFFSET $2`,
			limit, offset)
	} else {
		rows, err = p.pool.Query(ctx,
			`SELECT `+cols+` FROM jobs WHERE status=$1 ORDER BY updated_at DESC LIMIT $2 OFFSET $3`,
			string(status), limit, offset)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgScanJobs(rows)
}

func (p *PostgresStorage) RetryJob(ctx context.Context, id string) error {
	_, err := p.pool.Exec(ctx, `
		UPDATE jobs
		SET status='pending', run_at=NOW(), attempts=0, last_error='', updated_at=NOW()
		WHERE id=$1 AND status='failed'`,
		id,
	)
	return err
}

func pgScanJobs(rows pgx.Rows) ([]*Job, error) {
	var out []*Job
	for rows.Next() {
		j := &Job{}
		var status string
		var lastError *string
		if err := rows.Scan(
			&j.ID, &j.Type, &j.Payload, &status,
			&j.Attempts, &j.MaxRetries,
			&j.RunAt, &j.CreatedAt, &j.UpdatedAt, &lastError,
		); err != nil {
			return nil, err
		}
		j.Status = Status(status)
		if lastError != nil {
			j.LastError = *lastError
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

func pgScanCrons(rows pgx.Rows) ([]*CronEntry, error) {
	var out []*CronEntry
	for rows.Next() {
		c := &CronEntry{}
		var lastRun *time.Time
		if err := rows.Scan(&c.Name, &c.Schedule, &lastRun, &c.NextRun, &c.CreatedAt); err != nil {
			return nil, err
		}
		c.LastRun = lastRun
		out = append(out, c)
	}
	return out, rows.Err()
}
