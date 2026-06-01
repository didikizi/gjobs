package gjobs

import (
	"context"
	"time"
)

// Storage is the persistence layer. Implement this interface to add a new backend
// (e.g. MySQL, Redis) without changing any other code.
type Storage interface {
	Enqueue(ctx context.Context, job *Job) error
	// EnqueueDedup inserts a job whose DedupKey is non-empty, applying the given
	// deduplication mode. Behavior is unspecified when job.DedupKey == "".
	// Returns an EnqueueResult describing what happened (inserted, replaced, skipped).
	EnqueueDedup(ctx context.Context, job *Job, mode DedupMode) (EnqueueResult, error)
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

// DedupMode controls how a duplicate is handled by EnqueueDedup.
type DedupMode int

const (
	// DedupModeIgnore: if a duplicate exists, skip the new enqueue.
	// This is the default applied by DedupKey().
	DedupModeIgnore DedupMode = iota
	// DedupModeReplace: if a duplicate pending or TTL-locked completed job
	// exists, replace it with the new one. A running duplicate cannot be
	// replaced; the enqueue is skipped (running job will cover this enqueue).
	DedupModeReplace
)

// EnqueueAction describes the outcome of an EnqueueDedup call.
type EnqueueAction int

const (
	// EnqueueInserted: the job was inserted (no conflict).
	EnqueueInserted EnqueueAction = iota
	// EnqueueReplaced: a non-running duplicate was replaced (Replace mode).
	EnqueueReplaced
	// EnqueueSkippedDuplicate: a duplicate was found and skipped (Ignore mode).
	EnqueueSkippedDuplicate
	// EnqueueSkippedRunning: Replace mode found a duplicate currently running
	// and skipped the new enqueue (the running job will cover this request).
	EnqueueSkippedRunning
)

// String returns a snake_case label suitable for structured logging.
func (a EnqueueAction) String() string {
	switch a {
	case EnqueueInserted:
		return "inserted"
	case EnqueueReplaced:
		return "replaced"
	case EnqueueSkippedDuplicate:
		return "skipped_duplicate"
	case EnqueueSkippedRunning:
		return "skipped_running"
	default:
		return "unknown"
	}
}

// EnqueueResult is returned by Storage.EnqueueDedup. When Action is one of the
// non-Inserted values, ExistingJobID and ExistingStatus describe the conflicting row.
type EnqueueResult struct {
	Action         EnqueueAction
	ExistingJobID  string
	ExistingStatus Status
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
