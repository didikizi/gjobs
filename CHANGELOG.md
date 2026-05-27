# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - 2025-05-26

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
