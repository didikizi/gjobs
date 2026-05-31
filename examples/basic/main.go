// Package main is a minimal gjobs example.
// Run: go run ./examples/basic
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/didikizi/gjobs"
)

type Email struct {
	To, Subject string
}

var SendEmail = jobs.Def("send_email")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	q, err := jobs.New(jobs.WithDB("example.db"))
	if err != nil {
		log.Fatal(err)
	}

	jobs.HandleDef[Email](q, SendEmail, func(_ context.Context, e Email) error {
		fmt.Printf("→ %s: %s\n", e.To, e.Subject)
		return nil
	})

	if err := q.Enqueue(ctx, SendEmail, Email{To: "alice@example.com", Subject: "Welcome!"}); err != nil {
		log.Fatal(err)
	}

	if err := q.Start(ctx); err != nil {
		log.Println("queue stopped:", err)
	}
}
