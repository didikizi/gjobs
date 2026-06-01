// Package main demonstrates custom logging, the error channel, and unlimited retries.
// Run: go run ./examples/errors
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/didikizi/gjobs"
)

var (
	// Transient job: unlimited retries until it succeeds.
	Sync = gjobs.Def("sync").WithAttempts(gjobs.Unlimited)

	// Always-failing job: 3 retries, then dead-lettered.
	Flaky = gjobs.Def("flaky")
)

func main() {
	// *slog.Logger satisfies gjobs.Logger directly — no adapter needed.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// Error channel — sized so it won't block workers under normal load.
	errCh := make(chan gjobs.JobError, 64)

	q, err := gjobs.New(
		gjobs.WithStorage(gjobs.NewMemoryStorage()),
		gjobs.WithLogger(logger),
		gjobs.WithErrorChannel(errCh),
		gjobs.WithConcurrency(4),
	)
	if err != nil {
		panic(err)
	}

	// ── Handlers ──────────────────────────────────────────────────────────────

	attempts := 0
	q.Register(Sync, func(ctx context.Context, _ []byte) error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("sync: transient error (attempt %d)", attempts)
		}
		fmt.Printf("  [sync] succeeded after %d attempts\n", attempts)
		return nil
	})

	q.Register(Flaky, func(_ context.Context, _ []byte) error {
		return errors.New("flaky: always fails")
	})

	// ── Error channel consumer ────────────────────────────────────────────────

	go func() {
		for e := range errCh {
			if e.Final {
				// Dead-lettered — trigger your alerting here.
				fmt.Printf("  [dead-letter] job=%s type=%s err=%v\n", e.JobID, e.Type, e.Err)
			} else {
				fmt.Printf("  [retry] job=%s type=%s attempt=%d err=%v\n",
					e.JobID, e.Type, e.Attempt, e.Err)
			}
		}
	}()

	// ── Enqueue ───────────────────────────────────────────────────────────────

	// Sync will retry until handler succeeds (unlimited retries).
	// Backoff makes retries happen after 30s+, so for the demo we start it
	// immediately and note that in a real service the retries happen over time.
	_ = q.Enqueue(context.Background(), Sync, nil)

	// Flaky will fail 3 times and end up dead-lettered.
	_ = q.Enqueue(context.Background(), Flaky, nil)

	fmt.Println("Running — watching error channel. Press Ctrl+C to stop.")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Give jobs a moment to run before shutdown in this demo.
	go func() {
		time.Sleep(5 * time.Second)
		stop()
	}()

	if err := q.Start(ctx); err != nil {
		panic(err)
	}

	close(errCh)
}
