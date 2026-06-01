package gjobs

import (
	"fmt"
	"log"
	"strings"
	"time"
)

// Logger is the interface for queue-level logging. The signature matches
// *slog.Logger exactly — pass slog.Default() directly without any adapter.
//
//	gjobs.New(gjobs.WithLogger(slog.Default()))
//	gjobs.New(gjobs.WithNoLogger(), gjobs.WithErrorChannel(ch))
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type stdLogger struct{}

func (stdLogger) Info(msg string, args ...any)  { log.Println("[gjobs] INFO  " + msg + formatKV(args)) }
func (stdLogger) Warn(msg string, args ...any)  { log.Println("[gjobs] WARN  " + msg + formatKV(args)) }
func (stdLogger) Error(msg string, args ...any) { log.Println("[gjobs] ERROR " + msg + formatKV(args)) }

// formatKV formats key-value pairs as " k=v k=v" for the stdlib logger.
func formatKV(args []any) string {
	if len(args) == 0 {
		return ""
	}
	var sb strings.Builder
	for i := 0; i+1 < len(args); i += 2 {
		fmt.Fprintf(&sb, " %v=%v", args[i], args[i+1])
	}
	if len(args)%2 != 0 {
		fmt.Fprintf(&sb, " %v", args[len(args)-1])
	}
	return sb.String()
}

type noopLogger struct{}

func (noopLogger) Info(_ string, _ ...any)  {}
func (noopLogger) Warn(_ string, _ ...any)  {}
func (noopLogger) Error(_ string, _ ...any) {}

// config holds Queue-level settings applied via Option.
type config struct {
	dbPath          string
	concurrency     int
	pollInterval    time.Duration
	backoffBase     time.Duration
	backoffCap      time.Duration
	shutdownTimeout time.Duration
	storage         Storage
	logger          Logger
	errCh           chan<- JobError
}

func defaultConfig() config {
	return config{
		dbPath:       "jobs.db",
		concurrency:  10,
		pollInterval: 500 * time.Millisecond,
		backoffBase:  30 * time.Second,
		backoffCap:   1 * time.Hour,
		logger:       stdLogger{},
	}
}

// Option configures the Queue at construction time.
type Option func(*config)

// WithDB sets the SQLite database file path (default: "jobs.db").
func WithDB(path string) Option {
	return func(c *config) { c.dbPath = path }
}

// WithConcurrency sets the maximum number of jobs that run in parallel (default: 10).
func WithConcurrency(n int) Option {
	return func(c *config) { c.concurrency = n }
}

// WithPollInterval sets how often the worker checks for new jobs (default: 500ms).
func WithPollInterval(d time.Duration) Option {
	return func(c *config) { c.pollInterval = d }
}

// WithBackoffBase sets the initial retry delay (default: 30s).
// The actual delay for attempt n is: min(base * 2^(n-1), cap).
func WithBackoffBase(d time.Duration) Option {
	return func(c *config) { c.backoffBase = d }
}

// WithBackoffCap sets the maximum retry delay (default: 1h).
func WithBackoffCap(d time.Duration) Option {
	return func(c *config) { c.backoffCap = d }
}

// WithShutdownTimeout sets a deadline for graceful shutdown when Start's context
// is cancelled. Workers that exceed this deadline are abandoned (jobs stay pending
// and recover on next restart via RecoverStuck). Default: wait indefinitely.
//
//	q, _ := gjobs.New(gjobs.WithShutdownTimeout(30 * time.Second))
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *config) { c.shutdownTimeout = d }
}

// WithStorage injects a custom Storage backend, bypassing the default SQLite one.
// Useful for testing (MemoryStorage) or alternative databases (PostgresStorage).
func WithStorage(s Storage) Option {
	return func(c *config) { c.storage = s }
}

// WithLogger sets a custom logger. The Logger interface matches *slog.Logger exactly,
// so slog.Default() can be passed directly.
//
//	gjobs.New(gjobs.WithLogger(slog.Default()))
func WithLogger(l Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithNoLogger disables all log output. Combine with WithErrorChannel to
// handle errors programmatically without any console output.
//
//	errCh := make(chan gjobs.JobError, 64)
//	gjobs.New(gjobs.WithNoLogger(), gjobs.WithErrorChannel(errCh))
func WithNoLogger() Option {
	return func(c *config) { c.logger = noopLogger{} }
}

// WithErrorChannel sets a channel that receives a JobError whenever a job
// fails. The send is non-blocking — if the channel is full the error is
// logged and dropped, so size the channel appropriately.
//
//	errCh := make(chan gjobs.JobError, 64)
//	q, _ := gjobs.New(gjobs.WithErrorChannel(errCh))
//	go func() {
//	    for e := range errCh {
//	        if e.Final { alerting.DeadLetter(e) }
//	    }
//	}()
func WithErrorChannel(ch chan<- JobError) Option {
	return func(c *config) { c.errCh = ch }
}

// ── Push options ──────────────────────────────────────────────────────────────

// pushConfig holds per-job push settings.
type pushConfig struct {
	maxAttempts int
	runAt       time.Time
	dedupKey    string
	dedupMode   DedupMode
	dedupTTL    time.Duration
}

func defaultPushConfig() pushConfig {
	return pushConfig{maxAttempts: 3, runAt: time.Now()}
}

// PushOption configures an individual job at push time.
type PushOption func(*pushConfig)

// Attempts sets the total number of execution attempts for this push (default: 3).
// Pass gjobs.Unlimited (-1) to retry indefinitely.
//
//	q.Enqueue(ctx, job, data, gjobs.Attempts(gjobs.Unlimited))
func Attempts(n int) PushOption {
	return func(c *pushConfig) { c.maxAttempts = n }
}

// After delays job execution by d relative to now.
func After(d time.Duration) PushOption {
	return func(c *pushConfig) { c.runAt = time.Now().Add(d) }
}

// At schedules the job to run at an absolute time.
func At(t time.Time) PushOption {
	return func(c *pushConfig) { c.runAt = t }
}

// DedupKey marks the job with a deduplication key. By default (Ignore mode),
// if another job with the same key is currently pending, running, or completed
// with an active DedupTTL, the new enqueue is silently skipped — Enqueue returns
// nil and logs at WARN level.
//
// Use DedupReplace() to override pending duplicates instead of skipping them.
// Use DedupTTL(d) to keep the key locked for d after job completion.
//
// An empty key is a no-op — equivalent to no deduplication.
//
//	q.Enqueue(ctx, SendEmail, data, gjobs.DedupKey("welcome:user-42"))
func DedupKey(key string) PushOption {
	return func(c *pushConfig) { c.dedupKey = key }
}

// DedupReplace switches deduplication mode from Ignore (default) to Replace.
// When a duplicate is found:
//   - if the existing job is pending or in a TTL-locked completed state, it
//     is deleted and the new job is enqueued
//   - if the existing job is running, the new enqueue is skipped (no error,
//     log at WARN). The running job will either succeed (no need to re-enqueue)
//     or fail and retry (covers the new request automatically).
//
// Requires DedupKey to take effect. Without DedupKey, this option is a no-op.
//
//	q.Enqueue(ctx, SendEmail, data, gjobs.DedupKey("k"), gjobs.DedupReplace())
func DedupReplace() PushOption {
	return func(c *pushConfig) { c.dedupMode = DedupModeReplace }
}

// DedupTTL extends the deduplication window past the job's completion. After
// the job reaches done or dead-letter status, the key remains locked for d.
// During this window, new enqueues with the same key are treated as duplicates
// (skipped under Ignore, replaced under Replace).
//
// Without DedupTTL, the key is freed immediately when the job completes.
// Time is measured from the moment of completion, not from enqueue.
//
// Storage granularity is one second; sub-second values round up to 1s.
//
// Requires DedupKey to take effect. Without DedupKey, this option is a no-op.
//
//	q.Enqueue(ctx, GenerateReport, data,
//	    gjobs.DedupKey("daily-report"), gjobs.DedupTTL(24*time.Hour))
func DedupTTL(d time.Duration) PushOption {
	return func(c *pushConfig) { c.dedupTTL = d }
}
