<div align="center">

# ⚡ jobs

**Background jobs for Go — without Redis, Docker, or YAML.**

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat-square&logo=go&logoColor=white)](https://go.dev)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](#license)
[![SQLite](https://img.shields.io/badge/storage-SQLite%20%7C%20Postgres%20%7C%20Memory-blue?style=flat-square)](#-storage-backends)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen?style=flat-square)](#-testing)

One file. One database. Zero infrastructure.

[Quick start](#-quick-start) · [Job definitions](#️-job-definitions) · [Storage](#-storage-backends) · [Benchmarks](#-benchmarks) · [API](#-api-reference)

</div>

---

## ✨ Quick start

```go
var SendEmail = jobs.Def("send_email")

q, _ := jobs.New()

jobs.HandleDef[Email](q, SendEmail, func(ctx context.Context, e Email) error {
    return smtp.Send(e)
})
q.Enqueue(SendEmail, Email{To: "user@example.com"})

q.Start(ctx)
```

Job persists across restarts, retries on failure, shuts down cleanly — **zero configuration**.

---

## 📦 Installation

```bash
go get github.com/vkorolev/gjobs
```

> **Requirements:** Go 1.21+. The default SQLite backend requires CGO (GCC in `PATH`).  
> Use `MemoryStorage` or `PostgresStorage` for CGO-free builds.

---

## 🏷️ Job definitions

Define your jobs **once as typed variables** — no magic strings, no repeated options.

```go
var (
    SendEmail      = jobs.Def("send_email")
    ChargeCard     = jobs.Def("charge_card").WithRetries(10).WithTimeout(2*time.Minute)
    GenerateReport = jobs.Def("generate_report").WithTimeout(15*time.Minute)
    Heartbeat      = jobs.Def("heartbeat")
)
```

`Def` returns a `JobDef` with 3 retries and no timeout. Customise with chainable methods:

| Method | Description |
|--------|-------------|
| `.WithRetries(n)` | Set max retry attempts |
| `.WithTimeout(d)` | Cancel handler after duration |
| `.WithBackoff(base, cap)` | Override retry delay for this job |

---

## 🔧 Handlers

### Typed generics `HandleDef` (recommended)

```go
jobs.HandleDef[Email](q, SendEmail, func(ctx context.Context, e Email) error {
    return smtp.Send(e)
})
```

### Raw bytes (maximum control)

```go
q.Register(SendEmail, func(ctx context.Context, payload []byte) error {
    var e Email
    json.Unmarshal(payload, &e)
    return smtp.Send(e)
})
```

---

## 📬 Pushing jobs

```go
// Uses defaults (3 retries, no timeout)
q.Enqueue(SendEmail, Email{To: "alice@example.com", Subject: "Hi!"})

// Override retries on a single push
q.Enqueue(ChargeCard, payment, jobs.Retries(15))

// Delayed — run after 10 minutes
q.Enqueue(SendEmail, data, jobs.After(10*time.Minute))

// Scheduled — run at an exact time
q.Enqueue(GenerateReport, data, jobs.At(billingDate))
```

---

## 🔁 Retries & dead-letter

Failed jobs retry with **exponential backoff**: `base × 2^attempt`, capped at `cap`.

| Attempt | Default delay (base=30s, cap=1h) |
|:-------:|:--------------------------------:|
| 1 | 30s |
| 2 | 1m |
| 3 | 2m |
| 4 | 4m |
| 5 | 8m |
| … | … (max 1h) |

After all retries are exhausted the job moves to the **dead-letter queue**
(`status = 'failed'`) where it can be inspected or re-queued.

The **dashboard** shows the exact scheduled time alongside a relative countdown
("in 4m 32s") for any pending job that is waiting before its next attempt.

### Configuring backoff

**Queue-level defaults** (apply to all jobs unless overridden):

```go
q, _ := jobs.New(
    jobs.WithBackoffBase(1 * time.Minute), // default: 30s
    jobs.WithBackoffCap(6 * time.Hour),    // default: 1h
)
```

**Per-job override** via `JobDef`:

```go
// Aggressive retry for lightweight jobs
var Notify = jobs.Def("notify").WithBackoff(5*time.Second, 5*time.Minute)

// Slow retry for heavy background work
var HeavySync = jobs.Def("heavy_sync").WithBackoff(2*time.Minute, 12*time.Hour)
```

### Unlimited retries

Pass `jobs.Unlimited` (`-1`) to retry indefinitely until the job succeeds:

```go
q.Enqueue(Sync, data, jobs.Retries(jobs.Unlimited))

// or bake it into the JobDef
var Sync = jobs.Def("sync").WithRetries(jobs.Unlimited)
```

---

## ⏱️ Delayed & scheduled jobs

```go
// Run once, 5 minutes from now
q.Enqueue(Reminder, data, jobs.After(5*time.Minute))

// Run at a specific moment
q.Enqueue(Invoice, data, jobs.At(time.Date(2025, 12, 1, 9, 0, 0, 0, time.UTC)))
```

The scheduled time is stored in the database — survives restarts.

---

## 🕐 Cron jobs

```go
var Cleanup = jobs.Def("cleanup")

q.Schedule(Cleanup, "1h", func(ctx context.Context) error {
    return db.DeleteExpired()
})
```

Schedule format: any Go duration string — `"5s"`, `"30m"`, `"2h"`, `"24h"`.

Cron state persists in the database. Missed runs fire once on restart.

---

## 🗄️ Storage backends

| Backend | Persistence | Multi-process | Use case |
|---------|:-----------:|:-------------:|----------|
| **SQLite** (default) | ✅ | ❌ | Single-machine production |
| **Memory** | ❌ | ❌ | Unit tests, local development |
| **PostgreSQL** | ✅ | ✅ | High-throughput, multi-instance |

### SQLite (default)

```go
q, _ := jobs.New()                             // → jobs.db in cwd
q, _ := jobs.New(jobs.WithDB("/data/jobs.db")) // custom path
```

WAL mode enabled. One writer at a time.

### Memory

```go
q, _ := jobs.New(jobs.WithStorage(jobs.NewMemoryStorage()))
```

No disk, no CGO. Jobs lost on exit. Ideal for tests.

### PostgreSQL

```go
pg, err := jobs.NewPostgresStorage(ctx, "postgres://user:pass@host/db?sslmode=disable")
q, _ := jobs.New(jobs.WithStorage(pg))
```

Uses `FOR UPDATE SKIP LOCKED` for concurrent claiming. Multiple processes can
share the same database safely.

---

## ⚙️ Configuration

```go
q, _ := jobs.New(
    jobs.WithDB("myapp.db"),
    jobs.WithConcurrency(20),
    jobs.WithPollInterval(200 * time.Millisecond),
)
```

| Option | Default | Description |
|--------|:-------:|-------------|
| `WithDB(path)` | `"jobs.db"` | SQLite file path |
| `WithConcurrency(n)` | `10` | Max parallel handlers |
| `WithPollInterval(d)` | `500ms` | Storage poll cadence |
| `WithStorage(s)` | — | Custom storage backend |
| `WithBackoffBase(d)` | `30s` | Initial retry delay |
| `WithBackoffCap(d)` | `1h` | Maximum retry delay |
| `WithLogger(l)` | `stdLogger` (stdout) | Custom logger |
| `WithNoLogger()` | — | Silence all log output |
| `WithErrorChannel(ch)` | — | Receive job errors programmatically |

---

## 🪵 Logging

By default the library logs to `log.Printf`. Swap in any logger that satisfies the interface:

```go
type Logger interface {
    Info(msg string, args ...any)
    Error(msg string, args ...any)
}
```

**Use `log/slog`:**

```go
q, _ := jobs.New(jobs.WithLogger(slog.Default()))
```

**Use zap** (via a thin adapter):

```go
type zapAdapter struct{ *zap.SugaredLogger }
func (z zapAdapter) Info(msg string, args ...any)  { z.SugaredLogger.Infof(msg, args...) }
func (z zapAdapter) Error(msg string, args ...any) { z.SugaredLogger.Errorf(msg, args...) }

q, _ := jobs.New(jobs.WithLogger(zapAdapter{sugar}))
```

**Silence all output** and handle errors yourself via the error channel:

```go
q, _ := jobs.New(jobs.WithNoLogger(), jobs.WithErrorChannel(errCh))
```

---

## 📡 Error channel

Receive a `JobError` on every failure without relying on log output:

```go
errCh := make(chan jobs.JobError, 64)

q, _ := jobs.New(jobs.WithErrorChannel(errCh))

go func() {
    for e := range errCh {
        if e.Final {
            alerting.DeadLetter(e) // no more retries — take action
        } else {
            metrics.Inc("job.retry", e.Type)
        }
    }
}()
```

`JobError` fields:

| Field | Type | Description |
|-------|------|-------------|
| `JobID` | `string` | Database ID of the job |
| `Type` | `string` | Job type name |
| `Err` | `error` | Error returned by the handler |
| `Attempt` | `int` | Which attempt failed (1-based) |
| `Final` | `bool` | `true` when the job is dead-lettered |

The send is **non-blocking** — if the channel is full the error is logged and dropped,
so size it appropriately for your failure rate.

---

## 🖥️ Web dashboard

Start the built-in dashboard with a single call:

```go
srv, err := q.Dashboard(":8080")
```

Open `http://localhost:8080` to see:

- **Live stats** — pending / running / done / failed counts (auto-refresh every 5 s)
- **Job table** — filterable by status, paginated (50 per page)
- **Retry button** — re-queue any failed job from the UI

The dashboard server shuts down automatically when the queue stops. It is an optional, zero-dependency feature — just stdlib `net/http` and `html/template`.

Custom storage backends unlock the dashboard by implementing three extra methods:

```go
type DashboardStorage interface {
    Stats(ctx context.Context) (JobStats, error)
    Jobs(ctx context.Context, status Status, limit, offset int) ([]*Job, error)
    RetryJob(ctx context.Context, id string) error
}
```

---

## ⏹️ Cancelling running jobs

Cancel all in-flight jobs of a specific type instantly:

```go
n := q.CancelAll(SendEmail)
fmt.Printf("cancelled %d running send_email jobs\n", n)
```

Each running handler receives `ctx.Err() == context.Canceled`. Normal retry logic applies — the job is rescheduled if retries remain, or dead-lettered if exhausted.

Pending jobs (not yet picked up by a worker) are not affected.

---

## 🛑 Graceful shutdown

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

q.Start(ctx) // blocks; shuts down cleanly on SIGTERM
```

`Start` cancels → stops polling → waits for in-flight handlers → closes storage.

---

## 📂 Examples

| Example | Description |
|---------|-------------|
| [examples/basic](examples/basic/main.go) | Minimal setup — typed handlers, cron, delayed jobs |
| [examples/jobdef](examples/jobdef/main.go) | `JobDef` with custom retries, timeouts, backoff |
| [examples/errors](examples/errors/main.go) | Custom logger (slog), error channel, unlimited retries |
| [examples/dashboard](examples/dashboard/main.go) | Web dashboard — live stats, retries, delayed jobs |
| [examples/postgres](examples/postgres/main.go) | PostgreSQL backend, multi-process setup |

---

## 📊 Benchmarks

> Intel Xeon E5-2697 v3 @ 2.60GHz · Go 1.26 · SQLite WAL mode · Linux (WSL2)

### Enqueue throughput

| Payload | Size | ops/sec | µs/op | B/op |
|---------|-----:|--------:|------:|-----:|
| Notification | 64 B | ~127k | 26µs | 1 104 |
| Email | ~700 B | ~155k | 27µs | 1 104 |
| Payment | ~400 B | ~141k | 26µs | 1 104 |
| Report params | ~5 KB | ~132k | 24µs | 1 104 |

Enqueue cost is dominated by SQLite write latency, not payload size. All job types write at ~130–155k jobs/sec.

### End-to-end lifecycle (push → handler → done)

| Payload | Workers | jobs/sec | µs/job | allocs/op |
|---------|:-------:|--------:|------:|----------:|
| Notification | 10 | ~20k | 160µs | 69 |
| Email | 10 | ~17k | 186µs | 76 |
| Payment | 20 | ~27k | 152µs | 75 |
| Report params | 5 | ~10k | 328µs | 81 |

### Concurrency scaling

| Workers | jobs/sec | µs/job |
|:-------:|--------:|------:|
| 1 | ~2.6k | 1 300µs |
| 10 | ~27k | 138µs |
| 50 | ~29k | 128µs |

10→50 workers gives only ~5% gain — the bottleneck is SQLite's single-writer lock,
not goroutine throughput. For higher parallelism switch to **PostgreSQL**.

---

## 🧪 Testing

### Unit tests (zero infrastructure)

```bash
go test ./...
```

Uses in-memory SQLite and `MemoryStorage` — runs anywhere.

### Integration tests with PostgreSQL

```bash
# Start Postgres
docker compose up -d

# Run tests
JOBS_TEST_POSTGRES="postgres://jobs:jobs@localhost:5432/jobs_test?sslmode=disable" \
  go test ./...

# Tear down
docker compose down
```

### Mock storage for your own tests

```go
import "github.com/vkorolev/gjobs/testutil"

mock := testutil.NewMockStorage()

mock.ClaimFn = func(ctx context.Context, limit int) ([]*jobs.Job, error) {
    return []*jobs.Job{{ID: "1", Type: "send_email", MaxRetries: 3}}, nil
}

q, _ := jobs.New(jobs.WithStorage(mock))

var SendEmail = jobs.Def("send_email")
q.Register(SendEmail, myHandler)

// ... run queue ...

if calls := mock.CallsFor("MarkDone"); len(calls) != 1 {
    t.Errorf("expected 1 MarkDone, got %d", len(calls))
}
```

---

## 📖 API reference

### Queue construction

```go
jobs.New(opts ...Option) (*Queue, error)
```

### Registration and dispatch

```go
// Registration
q.Register(def JobDef, handler HandlerFunc)
jobs.HandleDef[T](q *Queue, def JobDef, fn func(ctx, T) error)

// Enqueueing
q.Enqueue(def JobDef, payload any, opts ...PushOption) error

// Cron
q.Schedule(def JobDef, interval string, fn func(ctx) error) error

// Cancel all in-flight jobs of a type; returns count cancelled
q.CancelAll(def JobDef) int
```

### Push options

| Option | Default | Description |
|--------|:-------:|-------------|
| `Retries(n)` | `3` | Max retry attempts |
| `After(d)` | immediate | Delay by duration |
| `At(t)` | immediate | Schedule at absolute time |

### Storage backends

```go
jobs.NewSQLiteStorage(path string) (*SQLiteStorage, error)
jobs.NewMemoryStorage() *MemoryStorage
jobs.NewPostgresStorage(ctx context.Context, connStr string) (*PostgresStorage, error)
```

### Storage interface

Implement this to add any backend (MySQL, Redis, …):

```go
type Storage interface {
    Enqueue(ctx context.Context, job *Job) error
    Claim(ctx context.Context, limit int) ([]*Job, error)
    MarkDone(ctx context.Context, id string) error
    MarkFailed(ctx context.Context, id string, errMsg string, retryAt *time.Time) error
    MarkPending(ctx context.Context, id string, runAt time.Time) error
    UpsertCron(ctx context.Context, c *CronEntry) error
    DueCrons(ctx context.Context) ([]*CronEntry, error)
    UpdateCronRun(ctx context.Context, name string, last, next time.Time) error
    Close() error
}
```

---

## 🗺️ Roadmap

| Status | Feature |
|:------:|---------|
| ✅ | SQLite storage, worker pool, retries |
| ✅ | Delayed jobs, cron, graceful shutdown |
| ✅ | Memory + PostgreSQL backends |
| ✅ | Logger interface, error channel, unlimited retries |
| ✅ | Web dashboard (`q.Dashboard(":8080")`) |
| ✅ | `CancelAll` — cancel all running jobs of a type |
| 🔜 | Batch push, job priorities |
| 🔜 | MySQL backend |
| 🔜 | Stable API |

---

## 📄 License

MIT
