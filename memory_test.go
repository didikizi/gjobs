package gjobs

import "testing"

func memoryFactory(t *testing.T) Storage {
	t.Helper()
	s := NewMemoryStorage()
	t.Cleanup(func() { s.Close() })
	return s
}

func TestMemoryStorage(t *testing.T) {
	runStorageTests(t, memoryFactory)
}
