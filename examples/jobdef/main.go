// Package main demonstrates the JobDef template API.
// Run: go run ./examples/jobdef
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/didikizi/gjobs"
)

// ── Job types ─────────────────────────────────────────────────────────────────

type Email struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type Payment struct {
	UserID    string  `json:"user_id"`
	AmountUSD float64 `json:"amount_usd"`
}

type ReportParams struct {
	ReportID  string    `json:"report_id"`
	StartDate time.Time `json:"start_date"`
	EndDate   time.Time `json:"end_date"`
}

// ── JobDef definitions ────────────────────────────────────────────────────────
// Define once, use everywhere — no magic strings scattered across the codebase.

var (
	SendEmail      = gjobs.Def("send_email")
	ChargeCard     = gjobs.Def("charge_card").WithAttempts(10).WithTimeout(2 * time.Minute)
	GenerateReport = gjobs.Def("generate_report").WithTimeout(15 * time.Minute)
	Heartbeat      = gjobs.Def("heartbeat")

	// Critical emails get more retries and a longer timeout.
	CriticalEmail = gjobs.Def("critical_email").
			WithAttempts(8).
			WithTimeout(45 * time.Second)
)

func main() {
	q, err := gjobs.New(
		gjobs.WithStorage(gjobs.NewMemoryStorage()),
		gjobs.WithConcurrency(5),
	)
	if err != nil {
		log.Fatal(err)
	}

	// ── Register handlers ─────────────────────────────────────────────────────

	gjobs.HandleDef[Email](q, SendEmail, func(ctx context.Context, e Email) error {
		fmt.Printf("  [email]   → %s: %q\n", e.To, e.Subject)
		return nil
	})

	gjobs.HandleDef[Email](q, CriticalEmail, func(ctx context.Context, e Email) error {
		fmt.Printf("  [CRITICAL] → %s: %q\n", e.To, e.Subject)
		return nil
	})

	gjobs.HandleDef[Payment](q, ChargeCard, func(ctx context.Context, p Payment) error {
		fmt.Printf("  [charge]  → user=%s $%.2f\n", p.UserID, p.AmountUSD)
		return nil
	})

	gjobs.HandleDef[ReportParams](q, GenerateReport, func(ctx context.Context, r ReportParams) error {
		fmt.Printf("  [report]  → %s (%s – %s)\n",
			r.ReportID,
			r.StartDate.Format("2006-01-02"),
			r.EndDate.Format("2006-01-02"),
		)
		time.Sleep(50 * time.Millisecond) // simulate work
		return nil
	})

	// Cron: heartbeat every 2 seconds.
	if err := q.Schedule(context.Background(), Heartbeat, "2s", func(ctx context.Context) error {
		fmt.Println("  [cron]    ♥ heartbeat")
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// ── Enqueue jobs ──────────────────────────────────────────────────────────

	// Standard email — uses SendEmail defaults (3 retries, no timeout).
	_ = q.Enqueue(context.Background(), SendEmail, Email{
		To:      "alice@example.com",
		Subject: "Welcome!",
		Body:    "Hi Alice, thanks for signing up.",
	})

	// Critical email — uses CriticalEmail overrides (8 retries, 45s timeout).
	_ = q.Enqueue(context.Background(), CriticalEmail, Email{
		To:      "bob@example.com",
		Subject: "Your invoice is ready",
		Body:    "Please find attached your invoice.",
	})

	// Charge with per-call retry override.
	_ = q.Enqueue(context.Background(), ChargeCard, Payment{UserID: "u-42", AmountUSD: 99.00}, gjobs.Attempts(15))

	// Report delayed by 1 second.
	_ = q.Enqueue(context.Background(), GenerateReport, ReportParams{
		ReportID:  "monthly-2025-12",
		StartDate: time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC),
		EndDate:   time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
	}, gjobs.After(1*time.Second))

	fmt.Println("Jobs queued. Press Ctrl+C to stop.")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := q.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
