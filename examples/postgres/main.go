// Package main demonstrates the PostgreSQL backend.
//
// Prerequisites:
//
//	docker compose up -d   # starts postgres defined in docker-compose.yml
//
// Run:
//
//	JOBS_DSN="postgres://jobs:jobs@localhost:5432/jobs_test?sslmode=disable" \
//	    go run ./examples/postgres
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vkorolev/gjobs"
)

type Notification struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

var Notify = jobs.Def("notify")

func main() {
	dsn := os.Getenv("JOBS_DSN")
	if dsn == "" {
		dsn = "postgres://jobs:jobs@localhost:5432/jobs_test?sslmode=disable"
	}

	ctx := context.Background()

	pg, err := jobs.NewPostgresStorage(ctx, dsn)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}

	q, err := jobs.New(
		jobs.WithStorage(pg),
		jobs.WithConcurrency(20), // Postgres handles concurrent writers; scale up freely.
		jobs.WithPollInterval(200*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}

	jobs.HandleDef[Notification](q, Notify, func(ctx context.Context, n Notification) error {
		fmt.Printf("  → [%s] %s\n", n.UserID, n.Message)
		return nil
	})

	// Enqueue a batch of notifications.
	for i := range 5 {
		_ = q.Enqueue(Notify, Notification{
			UserID:  fmt.Sprintf("user-%d", i+1),
			Message: fmt.Sprintf("Hello from worker %d!", i+1),
		})
	}

	fmt.Println("PostgreSQL backend ready. Press Ctrl+C to stop.")
	fmt.Println("Tip: run multiple processes pointing at the same DB —")
	fmt.Println("     FOR UPDATE SKIP LOCKED ensures each job runs exactly once.")

	sigCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := q.Start(sigCtx); err != nil {
		log.Fatal(err)
	}
}
