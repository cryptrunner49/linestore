package store

import (
	"os"
	"testing"
)

func TestStore(t *testing.T) {
	path := "test.db"
	os.Remove(path)
	os.Remove(path + ".idx")

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	line, err := store.Set([]byte("value1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	value, err := store.Get(line)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("expected 'value1', got '%s'", value)
	}

	_, err = store.Get(999)
	if err == nil {
		t.Error("expected error on get for non-existent line, got nil")
	}
}

func TestPersistence(t *testing.T) {
	path := "test.db"
	os.Remove(path)
	os.Remove(path + ".idx")

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	line, err := store.Set([]byte("value1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	store.Close()

	store, err = NewStore(path)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store.Close()

	value, err := store.Get(line)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("expected 'value1', got '%s'", value)
	}
}

func TestList(t *testing.T) {
	path := "test.db"
	os.Remove(path)
	os.Remove(path + ".idx")

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Add test data
	_, err = store.Set([]byte("value1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	_, err = store.Set([]byte("value2"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}

	pairs, err := store.List()
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(pairs) != 2 {
		t.Errorf("expected 2 pairs, got %d", len(pairs))
	}
}

func TestPolish(t *testing.T) {
	path := "test.db"
	os.Remove(path)
	os.Remove(path + ".idx")

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	line1, err := store.Set([]byte("value1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	line2, err := store.Set([]byte("value2"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}

	err = store.Polish()
	if err != nil {
		t.Fatalf("polish failed: %v", err)
	}

	value, err := store.Get(line1)
	if err != nil {
		t.Fatalf("get line %d failed after polish: %v", line1, err)
	}
	if string(value) != "value1" {
		t.Errorf("expected 'value1', got '%s'", value)
	}
	value, err = store.Get(line2)
	if err != nil {
		t.Fatalf("get line %d failed after polish: %v", line2, err)
	}
	if string(value) != "value2" {
		t.Errorf("expected 'value2', got '%s'", value)
	}
}

func TestBackup(t *testing.T) {
	path := "test.db"
	backupFull := "test_full_backup.db"
	os.Remove(path)
	os.Remove(path + ".idx")
	os.Remove(backupFull)
	os.Remove(backupFull + ".idx")

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	_, err = store.Set([]byte("value1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	line2, err := store.Set([]byte("value2"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}

	err = store.Backup(backupFull, false)
	if err != nil {
		t.Fatalf("full backup failed: %v", err)
	}
	fullStore, err := NewStore(backupFull)
	if err != nil {
		t.Fatalf("failed to open full backup: %v", err)
	}
	defer fullStore.Close()
	value, err := fullStore.Get(line2)
	if err != nil {
		t.Fatalf("get from full backup failed: %v", err)
	}
	if string(value) != "value2" {
		t.Errorf("expected 'value2' in full backup, got '%s'", value)
	}
}
