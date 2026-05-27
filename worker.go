package jobs

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// workerPool polls storage for pending jobs and dispatches them to a
// bounded set of goroutines.
type workerPool struct {
	storage      Storage
	handlers     map[string]HandlerFunc
	defs         map[string]JobDef
	concurrency  int
	pollInterval time.Duration
	backoffBase  time.Duration
	backoffCap   time.Duration
	logger       Logger
	errCh        chan<- JobError

	sem  chan struct{} // semaphore: cap == concurrency
	wg   sync.WaitGroup
	done chan struct{}

	cancelMu sync.Mutex
	cancels  map[string]map[string]context.CancelFunc // type → jobID → cancel
}

func newWorkerPool(
	s Storage,
	handlers map[string]HandlerFunc,
	defs map[string]JobDef,
	concurrency int,
	pollInterval time.Duration,
	backoffBase time.Duration,
	backoffCap time.Duration,
	logger Logger,
	errCh chan<- JobError,
) *workerPool {
	return &workerPool{
		storage:      s,
		handlers:     handlers,
		defs:         defs,
		concurrency:  concurrency,
		pollInterval: pollInterval,
		backoffBase:  backoffBase,
		backoffCap:   backoffCap,
		logger:       logger,
		errCh:        errCh,
		sem:          make(chan struct{}, concurrency),
		done:         make(chan struct{}),
		cancels:      make(map[string]map[string]context.CancelFunc),
	}
}

func (p *workerPool) start() {
	p.wg.Add(1)
	go p.pollLoop()
}

func (p *workerPool) stop() {
	close(p.done)
}

func (p *workerPool) wait() {
	p.wg.Wait()
}

func (p *workerPool) pollLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.poll()
		}
	}
}

func (p *workerPool) poll() {
	free := p.concurrency - len(p.sem)
	if free <= 0 {
		return
	}

	ctx := context.Background()
	jobs, err := p.storage.Claim(ctx, free)
	if err != nil {
		p.logger.Error("claim error: %v", err)
		return
	}
	for _, job := range jobs {
		p.dispatch(job)
	}
}

func (p *workerPool) dispatch(job *Job) {
	p.sem <- struct{}{}
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()
		defer func() { <-p.sem }()

		fn, ok := p.handlers[job.Type]
		if !ok {
			ctx := context.Background()
			err := fmt.Errorf("no handler registered for type %q", job.Type)
			p.logger.Error("%v (id=%s)", err, job.ID)
			p.emit(JobError{JobID: job.ID, Type: job.Type, Err: err, Attempt: job.Attempts + 1, Final: true})
			_ = p.storage.MarkFailed(ctx, job.ID, err.Error(), nil)
			return
		}

		// Cancellable context — allows CancelAll to interrupt this job.
		jobCtx, cancel := context.WithCancel(context.Background())
		p.cancelMu.Lock()
		if p.cancels[job.Type] == nil {
			p.cancels[job.Type] = make(map[string]context.CancelFunc)
		}
		p.cancels[job.Type][job.ID] = cancel
		p.cancelMu.Unlock()
		defer func() {
			p.cancelMu.Lock()
			delete(p.cancels[job.Type], job.ID)
			p.cancelMu.Unlock()
			cancel()
		}()

		err := fn(jobCtx, job.Payload)
		if err == nil {
			if e := p.storage.MarkDone(jobCtx, job.ID); e != nil {
				p.logger.Error("mark done (id=%s): %v", job.ID, e)
			}
			return
		}

		ctx := context.Background()
		def := p.defs[job.Type]
		attempt := job.Attempts + 1
		if isRetryable(attempt, job.MaxRetries) {
			retryAt := time.Now().Add(p.calcBackoff(def, attempt))
			p.logger.Info("job %s (type=%s) attempt %d failed, retry at %s: %v",
				job.ID, job.Type, attempt, retryAt.Format(time.RFC3339), err)
			p.emit(JobError{JobID: job.ID, Type: job.Type, Err: err, Attempt: attempt, Final: false})
			if e := p.storage.MarkFailed(ctx, job.ID, err.Error(), &retryAt); e != nil {
				p.logger.Error("schedule retry (id=%s): %v", job.ID, e)
			}
		} else {
			p.logger.Error("job %s (type=%s) dead-lettered after %d attempts: %v",
				job.ID, job.Type, attempt, err)
			p.emit(JobError{JobID: job.ID, Type: job.Type, Err: err, Attempt: attempt, Final: true})
			if e := p.storage.MarkFailed(ctx, job.ID, err.Error(), nil); e != nil {
				p.logger.Error("mark failed (id=%s): %v", job.ID, e)
			}
		}
	}()
}

// cancelAll cancels the context of every running job of the given type.
// Returns the number of jobs whose contexts were cancelled.
func (p *workerPool) cancelAll(typeName string) int {
	p.cancelMu.Lock()
	defer p.cancelMu.Unlock()
	m := p.cancels[typeName]
	for _, cancel := range m {
		cancel()
	}
	n := len(m)
	delete(p.cancels, typeName)
	return n
}

// calcBackoff returns base * 2^attempt, capped at cap.
// Per-job overrides in def take precedence over pool-level defaults.
func (p *workerPool) calcBackoff(def JobDef, attempt int) time.Duration {
	base := p.backoffBase
	if def.BackoffBase > 0 {
		base = def.BackoffBase
	}
	cap := p.backoffCap
	if def.BackoffCap > 0 {
		cap = def.BackoffCap
	}
	d := time.Duration(float64(base) * math.Pow(2, float64(attempt)))
	if d > cap {
		return cap
	}
	return d
}

// emit sends a JobError to the error channel if one is configured.
// The send is non-blocking: a full channel logs a warning and drops the event.
func (p *workerPool) emit(e JobError) {
	if p.errCh == nil {
		return
	}
	select {
	case p.errCh <- e:
	default:
		p.logger.Error("error channel full, dropping event for job %s", e.JobID)
	}
}

// isRetryable returns true when the job should be retried.
// maxRetries < 0 means Unlimited.
func isRetryable(attempt, maxRetries int) bool {
	if maxRetries < 0 {
		return true
	}
	return attempt < maxRetries
}
