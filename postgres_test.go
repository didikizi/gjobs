package jobs

import (
	"context"
	"os"
	"testing"
)

// To run Postgres tests set the env var before running:
//
//	JOBS_TEST_POSTGRES="postgres://user:pass@localhost/testdb?sslmode=disable" go test ./...
func postgresFactory(t *testing.T) Storage {
	t.Helper()
	connStr := os.Getenv("JOBS_TEST_POSTGRES")
	if connStr == "" {
		t.Skip("JOBS_TEST_POSTGRES not set — skipping Postgres tests")
	}
	s, err := NewPostgresStorage(context.Background(), connStr)
	if err != nil {
		t.Fatalf("NewPostgresStorage: %v", err)
	}
	t.Cleanup(func() {
		// Clean up tables so tests don't interfere with each other.
		s.pool.Exec(context.Background(), `TRUNCATE jobs, cron_jobs`) //nolint:errcheck
		s.Close()
	})
	return s
}

func TestPostgresStorage(t *testing.T) {
	runStorageTests(t, postgresFactory)
}
