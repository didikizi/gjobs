package gjobs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Queue is the main entry point. Create one with New, register handlers with
// Register/HandleDef/Schedule, push work with Enqueue, then call Start.
type Queue struct {
	storage Storage
	cfg     config

	mu           sync.RWMutex
	handlers     map[string]HandlerFunc
	defs         map[string]JobDef
	pendingCrons []cronReg

	pool      *workerPool
	scheduler *cronScheduler
	stopDash  func(context.Context) // set by Dashboard()

	started  atomic.Bool
	stopOnce sync.Once
}

// New creates a Queue. Storage defaults to SQLite at "jobs.db".
// Override with WithDB, WithConcurrency, WithPollInterval, or WithStorage.
func New(opts ...Option) (*Queue, error) {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}

	var s Storage
	if cfg.storage != nil {
		s = cfg.storage
	} else {
		var err error
		s, err = NewSQLiteStorage(cfg.dbPath)
		if err != nil {
			return nil, err
		}
	}

	return &Queue{
		storage:  s,
		cfg:      cfg,
		handlers: make(map[string]HandlerFunc),
		defs:     make(map[string]JobDef),
	}, nil
}

// Register registers a handler for the given JobDef.
// If JobDef.Timeout > 0 the handler context is cancelled after that duration.
// Panics if called after Start — all handlers must be registered before starting.
//
//	var SendEmail = jobs.Def("send_email")
//	q.Register(SendEmail, handler)
func (q *Queue) Register(def JobDef, handler HandlerFunc) {
	if q.started.Load() {
		panic("jobs: Register called after Start — register all handlers before calling Start")
	}
	h := handler
	if def.Timeout > 0 {
		h = func(ctx context.Context, payload []byte) error {
			ctx, cancel := context.WithTimeout(ctx, def.Timeout)
			defer cancel()
			return handler(ctx, payload)
		}
	}
	q.mu.Lock()
	q.handlers[def.Name] = h
	q.defs[def.Name] = def
	q.mu.Unlock()
}

// HandleDef registers a typed handler that automatically unmarshals the JSON payload.
//
//	var SendEmail = jobs.Def("send_email")
//	jobs.HandleDef[Email](q, SendEmail, func(ctx context.Context, e Email) error {
//	    return sendEmail(e)
//	})
func HandleDef[T any](q *Queue, def JobDef, fn func(ctx context.Context, payload T) error) {
	q.Register(def, func(ctx context.Context, raw []byte) error {
		var v T
		if err := json.Unmarshal(raw, &v); err != nil {
			return fmt.Errorf("jobs: unmarshal payload for %q: %w", def.Name, err)
		}
		return fn(ctx, v)
	})
}

// Enqueue adds a job to the queue. Uses def.MaxAttempts as the default;
// caller options override it.
//
//	q.Enqueue(ctx, SendEmail, Email{To: "user@example.com"})
//	q.Enqueue(ctx, SendEmail, data, jobs.Attempts(10))       // override attempts
//	q.Enqueue(ctx, SendEmail, data, jobs.After(time.Minute)) // delayed
func (q *Queue) Enqueue(ctx context.Context, def JobDef, payload any, opts ...PushOption) error {
	merged := make([]PushOption, 0, 1+len(opts))
	merged = append(merged, Attempts(def.MaxAttempts))
	merged = append(merged, opts...)

	pcfg := defaultPushConfig()
	for _, o := range merged {
		o(&pcfg)
	}
	return q.enqueueRaw(ctx, def.Name, payload, pcfg)
}

// Schedule registers a recurring job that fires every interval.
// interval is any Go duration string: "5s", "30m", "2h".
//
//	var Heartbeat = jobs.Def("heartbeat")
//	q.Schedule(ctx, Heartbeat, "1m", func(ctx context.Context) error { ... })
func (q *Queue) Schedule(ctx context.Context, def JobDef, interval string, fn func(ctx context.Context) error) error {
	q.Register(def, func(ctx context.Context, _ []byte) error { return fn(ctx) })
	return q.registerCron(ctx, def.Name, interval)
}

// CancelAll cancels the context of every currently running job of the given type.
// Handlers receive ctx.Err() == context.Canceled and the normal retry logic applies.
// Pending jobs (not yet picked up by a worker) are not affected.
// Returns the number of in-flight jobs whose contexts were cancelled.
//
//	n := q.CancelAll(SendEmail)
//	fmt.Printf("cancelled %d running send_email jobs\n", n)
func (q *Queue) CancelAll(def JobDef) int {
	if q.pool == nil {
		return 0
	}
	return q.pool.cancelAll(def.Name)
}

// ── internals ─────────────────────────────────────────────────────────────────

func (q *Queue) enqueueRaw(ctx context.Context, name string, payload any, pcfg pushConfig) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("jobs: marshal payload: %w", err)
	}
	now := time.Now()
	return q.storage.Enqueue(ctx, &Job{
		ID:         uuid.New().String(),
		Type:       name,
		Payload:    raw,
		Status:     StatusPending,
		MaxAttempts: pcfg.maxAttempts,
		RunAt:      pcfg.runAt,
		CreatedAt:  now,
		UpdatedAt:  now,
	})
}

// registerCron stores a cron entry; buffers it if called before Start.
func (q *Queue) registerCron(ctx context.Context, name, schedule string) error {
	if q.scheduler != nil {
		return q.scheduler.register(ctx, name, schedule)
	}
	q.mu.Lock()
	q.pendingCrons = append(q.pendingCrons, cronReg{name: name, schedule: schedule})
	q.mu.Unlock()
	return nil
}

// ── lifecycle ─────────────────────────────────────────────────────────────────

// Start begins processing jobs and cron schedules. It blocks until the
// context is cancelled, then performs a graceful shutdown.
func (q *Queue) Start(ctx context.Context) error {
	q.mu.RLock()
	handlers := make(map[string]HandlerFunc, len(q.handlers))
	for k, v := range q.handlers {
		handlers[k] = v
	}
	defs := make(map[string]JobDef, len(q.defs))
	for k, v := range q.defs {
		defs[k] = v
	}
	pending := q.pendingCrons
	q.mu.RUnlock()

	if err := q.storage.RecoverStuck(context.Background()); err != nil {
		q.cfg.logger.Error("recover stuck jobs on startup", "error", err)
	}

	q.pool = newWorkerPool(q.storage, handlers, defs, q.cfg.concurrency, q.cfg.pollInterval,
		q.cfg.backoffBase, q.cfg.backoffCap, q.cfg.logger, q.cfg.errCh)

	q.scheduler = newCronScheduler(q.storage, func(name string) error {
		return q.enqueueRaw(context.Background(), name, nil, pushConfig{maxAttempts: 0, runAt: time.Now()})
	}, q.cfg.logger, q.cfg.pollInterval)
	for _, cr := range pending {
		if err := q.scheduler.register(context.Background(), cr.name, cr.schedule); err != nil {
			return err
		}
	}

	q.started.Store(true)
	q.pool.start()
	q.scheduler.start()

	<-ctx.Done()

	shutdownCtx := context.Background()
	if q.cfg.shutdownTimeout > 0 {
		var cancel context.CancelFunc
		shutdownCtx, cancel = context.WithTimeout(context.Background(), q.cfg.shutdownTimeout)
		defer cancel()
	}
	return q.Stop(shutdownCtx)
}

// Stop gracefully stops the queue, waiting for in-flight jobs to finish.
// Safe to call multiple times.
func (q *Queue) Stop(ctx context.Context) error {
	var err error
	q.stopOnce.Do(func() {
		if q.scheduler != nil {
			q.scheduler.stop()
			q.scheduler.wait()
		}
		if q.pool != nil {
			q.pool.stop()
			done := make(chan struct{})
			go func() {
				q.pool.wait()
				close(done)
			}()
			select {
			case <-done:
			case <-ctx.Done():
				err = ctx.Err()
			}
		}
		if q.stopDash != nil {
			q.stopDash(ctx)
		}
		q.storage.Close() //nolint:errcheck
	})
	return err
}

// cronReg buffers Schedule calls made before Start.
type cronReg struct {
	name     string
	schedule string
}
