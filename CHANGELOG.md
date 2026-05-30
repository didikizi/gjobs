# Changelog

All notable changes to this project will be documented in this file.

## [0.4.0] - 2026-05-30

### Breaking Changes

- **PostgreSQL backend removed.** The `github.com/didikizi/gjobs/postgres`
  subpackage and `examples/postgres` are deleted. gjobs is now a
  single-machine, SQLite-only library. Users who need multi-machine
  processing should use [River](https://riverqueue.com) or
  [asynq](https://github.com/hibiken/asynq).

### Changed

- **Minimum Go version lowered to 1.22** (was 1.25).
  Downgraded `modernc.org/sqlite` to v1.36.1 — the last version whose
  full transitive dependency graph stays within Go 1.22.
  Removed `pgx/v5` from the module entirely.
- **CI matrix** now tests on Go 1.22, 1.23, 1.24, and 1.25.
- **Project layout:** `Storage` interface, `DashboardStorage` interface,
  and `JobStats` extracted from `queue.go`/`dashboard.go` into a
  dedicated `storage.go` — the full storage contract is now in one place.
- **README** — removed `README_RU.md`; English README is the single
  source of truth.

### Fixed

- **Timing attack in dashboard `basicAuth`:** username and password
  comparisons now use `crypto/subtle.ConstantTimeCompare` instead of `==`.
- **Flaky test:** `TestRecoverStuck_QueueRestart` timeout raised 2s → 5s
  to prevent spurious failures under `-race` on loaded CI runners.

### Added

- `.github/social-preview.svg` — 1280×640 social preview image for the
  GitHub repository (upload via Settings → Social preview).

---

## [0.3.0] - 2026-05-27

### Breaking Changes

- **`Enqueue` and `Schedule` now accept `context.Context` as first argument.**
  Migrate: `q.Enqueue(def, payload)` → `q.Enqueue(ctx, def, payload)`
- **Logger interface uses key-value pairs (slog-style), not Printf format.**
  `*slog.Logger` now satisfies `jobs.Logger` directly — no adapter needed.
  If you used `stdLogger`-style adapters with `%v` placeholders, switch to k-v pairs.
- **`PostgresStorage` moved to `github.com/didikizi/gjobs/postgres` subpackage.**
  Migrate: `jobs.NewPostgresStorage(ctx, dsn)` → `postgres.New(ctx, dsn)`
  This keeps pgx out of the binary for users who only need SQLite.

### Fixed

- **Stuck jobs on crash:** `Start()` now calls `RecoverStuck()` which resets any
  jobs left in `running` state back to `pending`. Jobs survive `kill -9`.
- **Panic in handler:** A panicking handler no longer kills the worker goroutine
  or leaves the job stuck in `running`. The panic is caught, turned into an error,
  and the normal retry/dead-letter logic applies. `last_error` contains `"panic: ..."`.
- **Backoff formula:** Fixed `base × 2^attempt` → `base × 2^(attempt-1)` so that
  attempt 1 = 30s, attempt 2 = 1m, attempt 3 = 2m — matches the documented table.
- **`MarkDone` context race:** Bookkeeping calls (`MarkDone`, `MarkFailed`) now use
  `context.Background()` so a concurrent `CancelAll` can no longer prevent status
  updates from reaching storage.
- **Shutdown deadline:** `Start()` no longer calls `Stop(context.Background())`.
  Use the new `WithShutdownTimeout` option to set a graceful drain deadline.
- **Register after Start:** Calling `Register` after `Start` now panics with a
  clear message instead of silently having no effect.

### Added

- `Storage.RecoverStuck(ctx)` — new interface method; all built-in backends implement it.
- `WithShutdownTimeout(d)` option — configures the maximum time `Start` waits for
  in-flight jobs to finish before returning. Default: wait indefinitely.
- `WithDashboardAuth(username, password)` — HTTP Basic Auth for the dashboard.
- `GET /stats.json` endpoint on the dashboard — returns `JobStats` as JSON for
  health checks and Prometheus integrations.
- Cancellable context in `workerPool.poll()` — a hung `Claim` call is now
  interrupted immediately when the queue shuts down.

### Changed

- **SQLite driver:** Replaced `mattn/go-sqlite3` (CGO) with `modernc.org/sqlite`
  (pure Go). No GCC required. Enables `GOOS=linux GOARCH=amd64` cross-compilation
  from macOS and CGO-free Alpine container builds.
- **SQLite `Claim`:** Removed unnecessary `BEGIN`/`COMMIT` wrapper — the
  `UPDATE…RETURNING` statement is already atomic in SQLite.
- `go.mod` minimum Go version updated to reflect actual requirements.

---

## [0.2.0] - 2026-05-26

### Added
- SQLite storage backend (WAL mode, CGO via `mattn/go-sqlite3`)
- PostgreSQL storage backend (`FOR UPDATE SKIP LOCKED`, multi-process safe)
- In-memory storage backend for tests and local development
- `JobDef` typed job descriptor — `Def(name)` with chainable `.WithRetries()`, `.WithTimeout()`, `.WithBackoff()`
- `HandleDef[T]` generic helper for typed handler registration
- Exponential backoff with configurable base and cap (per-queue and per-job)
- `Unlimited` sentinel (`-1`) for infinite retries
- Delayed jobs — `After(d)` and `At(t)` push options
- Cron scheduler — persistent in database, fires missed runs on restart
- Graceful shutdown — waits for in-flight handlers before closing storage
- `CancelAll(def)` — cancel all running jobs of a given type
- Web dashboard — live stats, job table with pagination, retry button
- Error channel (`WithErrorChannel`) — receive `JobError` on every failure
- Logger interface — stdlib `log`, `slog`, zap adapter supported
- `testutil.MockStorage` — configurable mock for unit tests
