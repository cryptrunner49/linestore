package stone

import (
	"os"
	"testing"
)

func TestStore(t *testing.T) {
	path := "test.db"
	os.Remove(path)

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	err = store.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	value, err := store.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("expected 'value1', got '%s'", value)
	}

	err = store.Set([]byte("key1"), []byte("value2"))
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	value, err = store.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get after update failed: %v", err)
	}
	if string(value) != "value2" {
		t.Errorf("expected 'value2', got '%s'", value)
	}

	err = store.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	_, err = store.Get([]byte("key1"))
	if err == nil {
		t.Error("expected error on get after delete, got nil")
	}

	_, err = store.Get([]byte("key2"))
	if err == nil {
		t.Error("expected error on get for non-existent key, got nil")
	}
}

func TestPersistence(t *testing.T) {
	path := "test.db"
	os.Remove(path)

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	err = store.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	store.Close()

	store, err = NewStore(path)
	if err != nil {
		t.Fatalf("failed to reopen store: %v", err)
	}
	defer store.Close()

	value, err := store.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("expected 'value1', got '%s'", value)
	}
}

func TestPolish(t *testing.T) {
	path := "test.db"
	os.Remove(path)

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Set and delete some keys
	err = store.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	err = store.Set([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	err = store.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Polish the database
	err = store.Polish()
	if err != nil {
		t.Fatalf("polish failed: %v", err)
	}

	// Verify only key2 remains
	_, err = store.Get([]byte("key1"))
	if err == nil {
		t.Error("expected key1 to be deleted after polish")
	}
	value, err := store.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("get key2 failed after polish: %v", err)
	}
	if string(value) != "value2" {
		t.Errorf("expected 'value2', got '%s'", value)
	}

	// Check file size reduced (qualitatively)
	stat, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	if stat.Size() > 50 { // Rough estimate: [0][4][key2][6][value2] ≈ 15 bytes
		t.Errorf("file size %d seems too large after polish", stat.Size())
	}
}

func TestBackup(t *testing.T) {
	path := "test.db"
	backupFull := "test_full_backup.db"
	backupPolished := "test_polished_backup.db"
	os.Remove(path)
	os.Remove(backupFull)
	os.Remove(backupPolished)

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Set and delete some keys
	err = store.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	err = store.Set([]byte("key2"), []byte("value2"))
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}
	err = store.Delete([]byte("key1"))
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Full backup
	err = store.Backup(backupFull, false)
	if err != nil {
		t.Fatalf("full backup failed: %v", err)
	}
	fullStore, err := NewStore(backupFull)
	if err != nil {
		t.Fatalf("failed to open full backup: %v", err)
	}
	defer fullStore.Close()
	value, err := fullStore.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("get from full backup failed: %v", err)
	}
	if string(value) != "value2" {
		t.Errorf("expected 'value2' in full backup, got '%s'", value)
	}

	// Polished backup
	err = store.Backup(backupPolished, true)
	if err != nil {
		t.Fatalf("polished backup failed: %v", err)
	}
	polishedStore, err := NewStore(backupPolished)
	if err != nil {
		t.Fatalf("failed to open polished backup: %v", err)
	}
	defer polishedStore.Close()
	_, err = polishedStore.Get([]byte("key1"))
	if err == nil {
		t.Error("expected key1 to be absent in polished backup")
	}
	value, err = polishedStore.Get([]byte("key2"))
	if err != nil {
		t.Fatalf("get from polished backup failed: %v", err)
	}
	if string(value) != "value2" {
		t.Errorf("expected 'value2' in polished backup, got '%s'", value)
	}
}