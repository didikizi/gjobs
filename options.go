package jobs

import (
	"log"
	"time"
)

// Logger is the interface for queue-level logging. Satisfied by log/slog,
// zap (via a thin adapter), zerolog, and others.
//
//	jobs.New(jobs.WithLogger(slog.Default()))
//	jobs.New(jobs.WithNoLogger(), jobs.WithErrorChannel(ch))
type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type stdLogger struct{}

func (stdLogger) Info(msg string, args ...any)  { log.Printf("[jobs] INFO  "+msg, args...) }
func (stdLogger) Error(msg string, args ...any) { log.Printf("[jobs] ERROR "+msg, args...) }

type noopLogger struct{}

func (noopLogger) Info(_ string, _ ...any)  {}
func (noopLogger) Error(_ string, _ ...any) {}

// config holds Queue-level settings applied via Option.
type config struct {
	dbPath       string
	concurrency  int
	pollInterval time.Duration
	backoffBase  time.Duration
	backoffCap   time.Duration
	storage      Storage
	logger       Logger
	errCh        chan<- JobError
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
// The actual delay for attempt n is: min(base * 2^n, cap).
func WithBackoffBase(d time.Duration) Option {
	return func(c *config) { c.backoffBase = d }
}

// WithBackoffCap sets the maximum retry delay (default: 1h).
func WithBackoffCap(d time.Duration) Option {
	return func(c *config) { c.backoffCap = d }
}

// WithStorage injects a custom Storage backend, bypassing the default SQLite one.
// Useful for testing (MemoryStorage) or alternative databases (PostgresStorage).
func WithStorage(s Storage) Option {
	return func(c *config) { c.storage = s }
}

// WithLogger sets a custom logger. The interface is satisfied by log/slog,
// as well as zap, zerolog, logrus (via a thin adapter).
//
//	jobs.New(jobs.WithLogger(slog.Default()))
func WithLogger(l Logger) Option {
	return func(c *config) { c.logger = l }
}

// WithNoLogger disables all log output. Combine with WithErrorChannel to
// handle errors programmatically without any console output.
//
//	errCh := make(chan jobs.JobError, 64)
//	jobs.New(jobs.WithNoLogger(), jobs.WithErrorChannel(errCh))
func WithNoLogger() Option {
	return func(c *config) { c.logger = noopLogger{} }
}

// WithErrorChannel sets a channel that receives a JobError whenever a job
// fails. The send is non-blocking — if the channel is full the error is
// logged and dropped, so size the channel appropriately.
//
//	errCh := make(chan jobs.JobError, 64)
//	q, _ := jobs.New(jobs.WithErrorChannel(errCh))
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
	maxRetries int
	runAt      time.Time
}

func defaultPushConfig() pushConfig {
	return pushConfig{maxRetries: 3, runAt: time.Now()}
}

// PushOption configures an individual job at push time.
type PushOption func(*pushConfig)

// Retries sets the maximum number of retry attempts (default: 3).
// Pass jobs.Unlimited (-1) to retry indefinitely.
//
//	q.Push("job", data, jobs.Retries(jobs.Unlimited))
func Retries(n int) PushOption {
	return func(c *pushConfig) { c.maxRetries = n }
}

// After delays job execution by d relative to now.
func After(d time.Duration) PushOption {
	return func(c *pushConfig) { c.runAt = time.Now().Add(d) }
}

// At schedules the job to run at an absolute time.
func At(t time.Time) PushOption {
	return func(c *pushConfig) { c.runAt = t }
}
