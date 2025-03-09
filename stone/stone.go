package stone

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// Store represents the StoneKV key/value store with on-disk persistence.
type Store struct {
	file  *os.File          // File handle for the database
	index map[string]uint64 // In-memory index mapping keys to value offsets
	mu    sync.RWMutex      // Mutex for concurrent access
}

// NewStore initializes or opens a StoneKV store at the given file path.
func NewStore(path string) (*Store, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}

	store := &Store{
		file:  file,
		index: make(map[string]uint64),
	}

	err = store.buildIndex()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to build index: %v", err)
	}

	return store, nil
}

// buildIndex reads the file and constructs the in-memory index.
func (s *Store) buildIndex() error {
	_, err := s.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	for {
		startOffset, err := s.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		var typeByte byte
		err = binary.Read(s.file, binary.LittleEndian, &typeByte)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		var keyLen uint32
		err = binary.Read(s.file, binary.LittleEndian, &keyLen)
		if err != nil {
			return err
		}

		keyBytes := make([]byte, keyLen)
		_, err = s.file.Read(keyBytes)
		if err != nil {
			return err
		}
		keyStr := string(keyBytes)

		if typeByte == 0 { // Set record
			valLenOffset := uint64(startOffset) + 1 + 4 + uint64(keyLen)
			s.index[keyStr] = valLenOffset

			var valLen uint32
			err = binary.Read(s.file, binary.LittleEndian, &valLen)
			if err != nil {
				return err
			}
			_, err = s.file.Seek(int64(valLen), io.SeekCurrent)
			if err != nil {
				return err
			}
		} else if typeByte == 1 { // Delete record
			delete(s.index, keyStr)
		} else {
			return fmt.Errorf("invalid record type: %d", typeByte)
		}
	}
	return nil
}

// Set stores a key/value pair in the database.
func (s *Store) Set(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := make([]byte, 1+4+len(key)+4+len(value))
	record[0] = 0
	binary.LittleEndian.PutUint32(record[1:5], uint32(len(key)))
	copy(record[5:5+len(key)], key)
	binary.LittleEndian.PutUint32(record[5+len(key):9+len(key)], uint32(len(value)))
	copy(record[9+len(key):], value)

	_, err := s.file.Write(record)
	if err != nil {
		return fmt.Errorf("failed to write record: %v", err)
	}

	stat, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stat: %v", err)
	}
	startOffset := stat.Size() - int64(len(record))
	valLenOffset := uint64(startOffset) + 1 + 4 + uint64(len(key))

	s.index[string(key)] = valLenOffset
	return nil
}

// Get retrieves the value associated with a key.
func (s *Store) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	offset, ok := s.index[string(key)]
	if !ok {
		return nil, fmt.Errorf("key not found")
	}

	_, err := s.file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek: %v", err)
	}

	var valLen uint32
	err = binary.Read(s.file, binary.LittleEndian, &valLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read value length: %v", err)
	}

	value := make([]byte, valLen)
	_, err = s.file.Read(value)
	if err != nil {
		return nil, fmt.Errorf("failed to read value: %v", err)
	}

	return value, nil
}

// Delete removes a key from the database.
func (s *Store) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	record := make([]byte, 1+4+len(key))
	record[0] = 1
	binary.LittleEndian.PutUint32(record[1:5], uint32(len(key)))
	copy(record[5:], key)

	_, err := s.file.Write(record)
	if err != nil {
		return fmt.Errorf("failed to write delete record: %v", err)
	}

	delete(s.index, string(key))
	return nil
}

// Polish compacts the database by creating a new file with only active key/value pairs.
// It backs up the original file before replacing it with the polished version.
func (s *Store) Polish() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the current file path
	origPath := s.file.Name()

	// Create a backup before polishing
	backupPath := origPath + ".backup"
	err := s.backupTo(backupPath, false) // Full backup
	if err != nil {
		return fmt.Errorf("failed to create backup before polish: %v", err)
	}

	// Create a temporary file for the polished database
	tempPath := origPath + ".tmp"
	tempFile, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	defer tempFile.Close()

	// Write only active key/value pairs from the index
	for key, offset := range s.index {
		// Seek to the value in the original file
		_, err = s.file.Seek(int64(offset), io.SeekStart)
		if err != nil {
			return fmt.Errorf("failed to seek to value offset: %v", err)
		}

		// Read value length and value
		var valLen uint32
		err = binary.Read(s.file, binary.LittleEndian, &valLen)
		if err != nil {
			return fmt.Errorf("failed to read value length: %v", err)
		}
		value := make([]byte, valLen)
		_, err = s.file.Read(value)
		if err != nil {
			return fmt.Errorf("failed to read value: %v", err)
		}

		// Write set record to temp file
		keyBytes := []byte(key)
		record := make([]byte, 1+4+len(keyBytes)+4+len(value))
		record[0] = 0
		binary.LittleEndian.PutUint32(record[1:5], uint32(len(keyBytes)))
		copy(record[5:5+len(keyBytes)], keyBytes)
		binary.LittleEndian.PutUint32(record[5+len(keyBytes):9+len(keyBytes)], valLen)
		copy(record[9+len(keyBytes):], value)

		_, err = tempFile.Write(record)
		if err != nil {
			return fmt.Errorf("failed to write polished record: %v", err)
		}
	}

	// Close the original file and replace it with the temp file
	err = s.file.Close()
	if err != nil {
		return fmt.Errorf("failed to close original file: %v", err)
	}
	err = os.Rename(tempPath, origPath)
	if err != nil {
		return fmt.Errorf("failed to replace original file: %v", err)
	}

	// Reopen the polished file
	s.file, err = os.OpenFile(origPath, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to reopen polished file: %v", err)
	}

	// Rebuild the index (optional, since it’s still valid, but ensures consistency)
	err = s.buildIndex()
	if err != nil {
		return fmt.Errorf("failed to rebuild index after polish: %v", err)
	}

	return nil
}

// Backup creates a backup of the database at the specified path.
// If polished is true, only active key/value pairs are included; otherwise, it’s a full copy.
func (s *Store) Backup(path string, polished bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.backupTo(path, polished)
}

// backupTo is a helper function to create a backup (locked separately for Polish).
func (s *Store) backupTo(path string, polished bool) error {
	if polished {
		// Create a temp store at the backup path and write only active records
		backupFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return fmt.Errorf("failed to create backup file: %v", err)
		}
		defer backupFile.Close()

		for key, offset := range s.index {
			_, err = s.file.Seek(int64(offset), io.SeekStart)
			if err != nil {
				return fmt.Errorf("failed to seek to value offset: %v", err)
			}

			var valLen uint32
			err = binary.Read(s.file, binary.LittleEndian, &valLen)
			if err != nil {
				return fmt.Errorf("failed to read value length: %v", err)
			}
			value := make([]byte, valLen)
			_, err = s.file.Read(value)
			if err != nil {
				return fmt.Errorf("failed to read value: %v", err)
			}

			keyBytes := []byte(key)
			record := make([]byte, 1+4+len(keyBytes)+4+len(value))
			record[0] = 0
			binary.LittleEndian.PutUint32(record[1:5], uint32(len(keyBytes)))
			copy(record[5:5+len(keyBytes)], keyBytes)
			binary.LittleEndian.PutUint32(record[5+len(keyBytes):9+len(keyBytes)], valLen)
			copy(record[9+len(keyBytes):], value)

			_, err = backupFile.Write(record)
			if err != nil {
				return fmt.Errorf("failed to write backup record: %v", err)
			}
		}
	} else {
		// Full backup: copy the entire file
		src, err := os.Open(s.file.Name())
		if err != nil {
			return fmt.Errorf("failed to open source file: %v", err)
		}
		defer src.Close()

		dst, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return fmt.Errorf("failed to create backup file: %v", err)
		}
		defer dst.Close()

		_, err = io.Copy(dst, src)
		if err != nil {
			return fmt.Errorf("failed to copy file: %v", err)
		}
	}

	return nil
}

// Close closes the store and releases resources.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.file.Close()
	if err != nil {
		return fmt.Errorf("failed to close file: %v", err)
	}
	return nil
}