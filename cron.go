package gjobs

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// cronScheduler watches cron_jobs in storage and enqueues a regular job
// whenever next_run is reached.
type cronScheduler struct {
	storage      Storage
	enqueue      func(name string) error
	logger       Logger
	pollInterval time.Duration
	entries      map[string]time.Duration
	mu           sync.Mutex
	done         chan struct{}
	wg           sync.WaitGroup
}

func newCronScheduler(s Storage, enqueue func(name string) error, logger Logger, pollInterval time.Duration) *cronScheduler {
	return &cronScheduler{
		storage:      s,
		enqueue:      enqueue,
		logger:       logger,
		pollInterval: pollInterval,
		entries:      make(map[string]time.Duration),
		done:         make(chan struct{}),
	}
}

// register stores a cron entry and persists it.
func (cs *cronScheduler) register(ctx context.Context, name, schedule string) error {
	d, err := time.ParseDuration(schedule)
	if err != nil {
		return fmt.Errorf("jobs: invalid cron schedule %q: %w", schedule, err)
	}

	cs.mu.Lock()
	cs.entries[name] = d
	cs.mu.Unlock()

	entry := &CronEntry{
		Name:      name,
		Schedule:  schedule,
		NextRun:   time.Now().Add(d),
		CreatedAt: time.Now(),
	}
	return cs.storage.UpsertCron(ctx, entry)
}

// start launches the cron ticker goroutine.
func (cs *cronScheduler) start() {
	cs.wg.Add(1)
	go cs.loop()
}

// stop signals the scheduler to exit.
func (cs *cronScheduler) stop() {
	close(cs.done)
}

// wait blocks until the scheduler goroutine exits.
func (cs *cronScheduler) wait() {
	cs.wg.Wait()
}

func (cs *cronScheduler) loop() {
	defer cs.wg.Done()
	ticker := time.NewTicker(cs.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cs.done:
			return
		case <-ticker.C:
			cs.fire()
		}
	}
}

func (cs *cronScheduler) fire() {
	ctx := context.Background()
	due, err := cs.storage.DueCrons(ctx)
	if err != nil {
		cs.logger.Error("cron poll error", "error", err)
		return
	}

	for _, entry := range due {
		cs.mu.Lock()
		d, ok := cs.entries[entry.Name]
		cs.mu.Unlock()
		if !ok {
			continue // handler not registered in this process
		}

		now := time.Now()
		next := now.Add(d)

		if err := cs.storage.UpdateCronRun(ctx, entry.Name, now, next); err != nil {
			cs.logger.Error("update cron run error", "name", entry.Name, "error", err)
			continue
		}

		if err := cs.enqueue(entry.Name); err != nil {
			cs.logger.Error("enqueue cron job error", "name", entry.Name, "error", err)
		}
	}
}
