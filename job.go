package gjobs

import (
	"context"
	"time"
)

// Unlimited means the job will be retried indefinitely until it succeeds.
// Backoff grows up to 1 hour and then stays constant.
//
//	var Sync = jobs.Def("sync").WithAttempts(jobs.Unlimited)
const Unlimited = -1

// HandlerFunc processes a job. payload is raw JSON.
type HandlerFunc func(ctx context.Context, payload []byte) error

// Status represents the lifecycle state of a job.
type Status string

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
)

// Job is a single unit of work stored in the database.
type Job struct {
	ID          string
	Type        string
	Payload     []byte
	Status      Status
	Attempts    int
	MaxAttempts int
	RunAt       time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
	LastError   string
}

// JobError is sent to the error channel whenever a job execution fails.
// Use it to implement custom alerting, metrics, or dead-letter processing
// without relying on log output.
type JobError struct {
	JobID   string // database ID of the job
	Type    string // job type name
	Err     error  // error returned by the handler
	Attempt int    // which attempt failed (1 = first attempt)
	Final   bool   // true if the job is now dead-lettered (no more retries left)
}

// CronEntry is a recurring schedule stored in the database.
type CronEntry struct {
	Name      string
	Schedule  string // duration string: "1h", "30m", "5s"
	LastRun   *time.Time
	NextRun   time.Time
	CreatedAt time.Time
}

// ── JobDef ────────────────────────────────────────────────────────────────────

// JobDef is a reusable job descriptor that bundles a type name with execution
// defaults (max attempts, per-handler timeout, backoff). Define it once as a
// package-level variable and use it everywhere — no magic strings, no repeated
// options.
//
//	var SendEmail  = jobs.Def("send_email")
//	var ChargeCard = jobs.Def("charge_card").WithAttempts(10).WithTimeout(2*time.Minute)
//
// Then use it everywhere:
//
//	q.Register(SendEmail, handler)
//	q.Enqueue(ctx, SendEmail, Email{To: "user@example.com"})
//	q.CancelAll(SendEmail)
type JobDef struct {
	// Name is the unique job type identifier stored in the database.
	Name string

	// MaxAttempts is the total number of execution attempts before a job is
	// dead-lettered. Can be overridden per-push with Attempts(n).
	// Default: 3. Use Unlimited (-1) to retry indefinitely.
	MaxAttempts int

	// Timeout is the per-execution deadline passed to the handler context.
	// Zero means no timeout.
	Timeout time.Duration

	// BackoffBase and BackoffCap override the queue-level defaults for this
	// job type only. Zero means "use queue default".
	BackoffBase time.Duration
	BackoffCap  time.Duration
}

// Def creates a JobDef with the given name and default settings
// (MaxAttempts: 3, no timeout). Customise with the With* methods.
//
//	var SendEmail  = jobs.Def("send_email")
//	var ChargeCard = jobs.Def("charge_card").WithAttempts(10).WithTimeout(2*time.Minute)
func Def(name string) JobDef {
	return JobDef{Name: name, MaxAttempts: 3}
}

// WithAttempts returns a copy with MaxAttempts set to n.
// n is the total number of execution attempts (including the first).
// Pass Unlimited (-1) to retry indefinitely.
func (d JobDef) WithAttempts(n int) JobDef {
	d.MaxAttempts = n
	return d
}

// WithTimeout returns a copy with Timeout set to t.
func (d JobDef) WithTimeout(t time.Duration) JobDef {
	d.Timeout = t
	return d
}

// WithBackoff returns a copy with per-job backoff overrides.
// base is the initial delay (default: 30s), max is the maximum delay (default: 1h).
//
//	var HeavyJob = jobs.Def("heavy_job").WithBackoff(1*time.Minute, 6*time.Hour)
func (d JobDef) WithBackoff(base, max time.Duration) JobDef {
	d.BackoffBase = base
	d.BackoffCap = max
	return d
}
