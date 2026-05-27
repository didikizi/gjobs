// Package main is a minimal example of the jobs library.
// Run: go run ./examples/basic
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

// ── Job definitions ────────────────────────────────────────────────────────────
// Define once as package-level variables — no magic strings anywhere else.

var (
	SendWelcomeEmail   = jobs.Def("send_welcome_email")
	SendPromoEmail     = jobs.Def("send_promo_email")
	ChargeSubscription = jobs.Def("charge_subscription")
	GenerateReport     = jobs.Def("generate_report")
	Heartbeat          = jobs.Def("heartbeat")
)

// ── Payload types ──────────────────────────────────────────────────────────────

type Email struct {
	To      string
	Subject string
	Body    string
}

type Payment struct {
	UserID   string
	Amount   float64
	Currency string
}

type Report struct {
	ReportID string
	Format   string
}

func main() {
	q, err := jobs.New(
		jobs.WithDB("example.db"),
		jobs.WithConcurrency(5),
	)
	if err != nil {
		log.Fatal(err)
	}

	// ── Register handlers ────────────────────────────────────────────────────
	jobs.HandleDef[Email](q, SendWelcomeEmail, func(_ context.Context, e Email) error {
		fmt.Printf("[email]   → %s  |  %s\n", e.To, e.Subject)
		return nil
	})
	jobs.HandleDef[Email](q, SendPromoEmail, func(_ context.Context, e Email) error {
		fmt.Printf("[promo]   → %s  |  %s\n", e.To, e.Subject)
		return nil
	})
	jobs.HandleDef[Payment](q, ChargeSubscription, func(_ context.Context, p Payment) error {
		fmt.Printf("[payment] → user=%s  %.2f %s\n", p.UserID, p.Amount, p.Currency)
		return nil
	})
	jobs.HandleDef[Report](q, GenerateReport, func(_ context.Context, r Report) error {
		fmt.Printf("[report]  → id=%s format=%s\n", r.ReportID, r.Format)
		time.Sleep(50 * time.Millisecond)
		return nil
	})

	// ── Cron ─────────────────────────────────────────────────────────────────
	q.Schedule(context.Background(), Heartbeat, "3s", func(_ context.Context) error {
		fmt.Println("[cron]    → heartbeat")
		return nil
	})

	// ── Enqueue work ─────────────────────────────────────────────────────────
	q.Enqueue(context.Background(), SendWelcomeEmail, Email{To: "alice@example.com", Subject: "Welcome!"})
	q.Enqueue(context.Background(), SendPromoEmail, Email{To: "bob@example.com", Subject: "20% off this week"})
	q.Enqueue(context.Background(), ChargeSubscription, Payment{UserID: "u_42", Amount: 9.99, Currency: "USD"})
	q.Enqueue(context.Background(), GenerateReport, Report{ReportID: "rpt_001", Format: "csv"})

	// Delayed — run after 2 seconds.
	q.Enqueue(context.Background(), SendPromoEmail,
		Email{To: "charlie@example.com", Subject: "Delayed promo"},
		jobs.After(2*time.Second),
	)

	// Override retries for a single push.
	q.Enqueue(context.Background(), ChargeSubscription,
		Payment{UserID: "u_99", Amount: 19.99, Currency: "EUR"},
		jobs.Retries(15),
	)

	fmt.Println("Queue started — press Ctrl+C to stop gracefully.")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := q.Start(ctx); err != nil {
		log.Printf("queue stopped: %v", err)
	}
}
