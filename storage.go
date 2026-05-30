package jobs

import (
	"context"
	"time"
)

// Storage is the persistence layer. Implement this interface to add a new backend
// (e.g. MySQL, Redis) without changing any other code.
type Storage interface {
	Enqueue(ctx context.Context, job *Job) error
	Claim(ctx context.Context, limit int) ([]*Job, error)
	MarkDone(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, errMsg string, retryAt *time.Time) error
	MarkPending(ctx context.Context, id string, runAt time.Time) error
	// RecoverStuck resets jobs left in 'running' back to 'pending'.
	// Called once on Start to recover jobs that were in-flight when the process crashed.
	RecoverStuck(ctx context.Context) error

	UpsertCron(ctx context.Context, c *CronEntry) error
	DueCrons(ctx context.Context) ([]*CronEntry, error)
	UpdateCronRun(ctx context.Context, name string, last, next time.Time) error

	Close() error
}

// DashboardStorage extends Storage with listing and retry capabilities used
// by the web dashboard. All built-in backends (SQLite, Memory) implement this
// interface. Custom backends can add these three methods to unlock q.Dashboard().
type DashboardStorage interface {
	Stats(ctx context.Context) (JobStats, error)
	// Jobs returns jobs ordered by updated_at DESC. status="" means all statuses.
	Jobs(ctx context.Context, status Status, limit, offset int) ([]*Job, error)
	// RetryJob moves a failed job back to pending with attempts reset to 0.
	RetryJob(ctx context.Context, id string) error
}

// JobStats holds per-status job counts returned by DashboardStorage.Stats.
type JobStats struct {
	Pending int
	Running int
	Done    int
	Failed  int
}
