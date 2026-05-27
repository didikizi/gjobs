// Package main demonstrates the built-in web dashboard.
// Run: go run ./examples/dashboard
// Open: http://localhost:8080
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/vkorolev/gjobs"
)

// ── Job definitions ────────────────────────────────────────────────────────────

var (
	SendEmail   = jobs.Def("send_email")
	ChargeCard  = jobs.Def("charge_card").WithRetries(5).WithTimeout(10 * time.Second)
	SyncData    = jobs.Def("sync_data").WithRetries(jobs.Unlimited)
	FlakyReport = jobs.Def("flaky_report").WithRetries(2)
)

// ── Payloads ───────────────────────────────────────────────────────────────────

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

type ChargePayload struct {
	UserID string  `json:"user_id"`
	Amount float64 `json:"amount"`
}

func main() {
	q, err := jobs.New(
		jobs.WithStorage(jobs.NewMemoryStorage()),
		jobs.WithConcurrency(4),
		jobs.WithPollInterval(200*time.Millisecond),
	)
	if err != nil {
		log.Fatal(err)
	}

	// ── Register handlers ─────────────────────────────────────────────────────

	jobs.HandleDef[EmailPayload](q, SendEmail, func(_ context.Context, e EmailPayload) error {
		fmt.Printf("  [email]  → %s: %q\n", e.To, e.Subject)
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	jobs.HandleDef[ChargePayload](q, ChargeCard, func(_ context.Context, p ChargePayload) error {
		fmt.Printf("  [charge] → user=%s $%.2f\n", p.UserID, p.Amount)
		time.Sleep(80 * time.Millisecond)
		if rand.Intn(3) == 0 {
			return errors.New("gateway timeout")
		}
		return nil
	})

	q.Register(SyncData, func(_ context.Context, _ []byte) error {
		fmt.Println("  [sync]   → syncing…")
		time.Sleep(120 * time.Millisecond)
		return nil
	})

	q.Register(FlakyReport, func(_ context.Context, _ []byte) error {
		fmt.Println("  [report] → generating…")
		time.Sleep(60 * time.Millisecond)
		return errors.New("report service unavailable")
	})

	// ── Enqueue initial work ──────────────────────────────────────────────────

	for i := range 5 {
		_ = q.Enqueue(SendEmail, EmailPayload{
			To:      fmt.Sprintf("user%d@example.com", i+1),
			Subject: fmt.Sprintf("Hello from job %d", i+1),
		})
	}

	for i := range 4 {
		_ = q.Enqueue(ChargeCard, ChargePayload{
			UserID: fmt.Sprintf("u_%03d", i+1),
			Amount: float64(10*(i+1)) + 0.99,
		})
	}

	_ = q.Enqueue(SyncData, nil)

	// FlakyReport will fail and retry — visible in the dashboard.
	_ = q.Enqueue(FlakyReport, nil)

	// Delayed jobs — appear as "pending" with a future run_at time.
	_ = q.Enqueue(SendEmail,
		EmailPayload{To: "delayed@example.com", Subject: "Delayed message"},
		jobs.After(30*time.Second),
	)
	_ = q.Enqueue(ChargeCard,
		ChargePayload{UserID: "u_vip", Amount: 199.99},
		jobs.After(1*time.Minute),
	)

	// ── Cron: generate background noise every 3 seconds ───────────────────────

	if err := q.Schedule(jobs.Def("heartbeat"), "3s", func(_ context.Context) error {
		_ = q.Enqueue(SendEmail, EmailPayload{
			To:      "cron@example.com",
			Subject: fmt.Sprintf("Heartbeat at %s", time.Now().Format("15:04:05")),
		})
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// ── Dashboard ─────────────────────────────────────────────────────────────

	if _, err := q.Dashboard(":8080"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Dashboard → http://localhost:8080")
	fmt.Println("Press Ctrl+C to stop.")

	// ── Start ─────────────────────────────────────────────────────────────────

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := q.Start(ctx); err != nil {
		log.Printf("queue stopped: %v", err)
	}
}
