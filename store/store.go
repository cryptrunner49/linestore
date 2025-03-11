package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// Store represents the line/value store with on-disk persistence.
type Store struct {
	file      *os.File // File handle for the database
	indexFile *os.File // File handle for the index
	lineCount uint64   // Tracks total lines written
	mu        sync.RWMutex
}

// NewStore initializes or opens a store at the given file path.
func NewStore(path string) (*Store, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %v", err)
	}

	indexPath := path + ".idx"
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to open index file: %v", err)
	}

	store := &Store{
		file:      file,
		indexFile: indexFile,
		lineCount: 0,
	}

	err = store.countLines()
	if err != nil {
		file.Close()
		indexFile.Close()
		return nil, fmt.Errorf("failed to count lines: %v", err)
	}

	return store, nil
}

// countLines determines the total number of records in the file and validates the index.
func (s *Store) countLines() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	lineNum := uint64(0)
	for {
		var typeByte byte
		err = binary.Read(s.file, binary.LittleEndian, &typeByte)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read type byte: %v", err)
		}
		if typeByte != 0 {
			return fmt.Errorf("invalid record type %d at line %d", typeByte, lineNum)
		}

		var valLen uint32
		err = binary.Read(s.file, binary.LittleEndian, &valLen)
		if err != nil {
			return fmt.Errorf("failed to read value length: %v", err)
		}
		_, err = s.file.Seek(int64(valLen), io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to skip value: %v", err)
		}
		lineNum++
	}
	s.lineCount = lineNum

	// Validate index file length
	indexStat, err := s.indexFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat index file: %v", err)
	}
	expectedSize := int64(s.lineCount * 16) // 8 bytes lineNum + 8 bytes offset
	if indexStat.Size() != expectedSize {
		return fmt.Errorf("index file size %d does not match expected %d", indexStat.Size(), expectedSize)
	}

	return nil
}

// Set appends a value to the store and updates the index file.
func (s *Store) Set(value []byte) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write to data file
	record := make([]byte, 1+4+len(value))
	record[0] = 0 // Active record
	binary.LittleEndian.PutUint32(record[1:5], uint32(len(value)))
	copy(record[5:], value)

	dataOffset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to end of data file: %v", err)
	}
	_, err = s.file.Write(record)
	if err != nil {
		return 0, fmt.Errorf("failed to write record: %v", err)
	}
	err = s.file.Sync()
	if err != nil {
		return 0, fmt.Errorf("failed to sync data file: %v", err)
	}

	// Write to index file
	lineNum := s.lineCount
	indexEntry := make([]byte, 16)
	binary.LittleEndian.PutUint64(indexEntry[0:8], lineNum)
	binary.LittleEndian.PutUint64(indexEntry[8:16], uint64(dataOffset))
	_, err = s.indexFile.Write(indexEntry)
	if err != nil {
		return 0, fmt.Errorf("failed to write index entry: %v", err)
	}
	err = s.indexFile.Sync()
	if err != nil {
		return 0, fmt.Errorf("failed to sync index file: %v", err)
	}

	s.lineCount++
	return lineNum, nil
}

// Get retrieves the value at the specified line number using the index file.
func (s *Store) Get(line uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if line >= s.lineCount {
		return nil, fmt.Errorf("line %d exceeds total lines %d", line, s.lineCount)
	}

	// Seek to the index entry for the line
	indexOffset := int64(line * 16) // 16 bytes per entry
	_, err := s.indexFile.Seek(indexOffset, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to index offset %d: %v", indexOffset, err)
	}

	indexEntry := make([]byte, 16)
	n, err := io.ReadFull(s.indexFile, indexEntry)
	if err != nil || n != 16 {
		return nil, fmt.Errorf("failed to read index entry for line %d: %v", line, err)
	}

	dataOffset := binary.LittleEndian.Uint64(indexEntry[8:16])
	_, err = s.file.Seek(int64(dataOffset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to data offset %d: %v", dataOffset, err)
	}

	var typeByte byte
	err = binary.Read(s.file, binary.LittleEndian, &typeByte)
	if err != nil {
		return nil, fmt.Errorf("failed to read type byte at line %d: %v", line, err)
	}
	if typeByte != 0 {
		return nil, fmt.Errorf("invalid record type %d at line %d", typeByte, line)
	}

	var valLen uint32
	err = binary.Read(s.file, binary.LittleEndian, &valLen)
	if err != nil {
		return nil, fmt.Errorf("failed to read value length at line %d: %v", line, err)
	}
	if valLen > 1<<20 {
		return nil, fmt.Errorf("invalid value length %d at line %d", valLen, line)
	}

	value := make([]byte, valLen)
	n, err = io.ReadFull(s.file, value)
	if err != nil {
		return nil, fmt.Errorf("failed to read value at line %d (read %d/%d bytes): %v", line, n, valLen, err)
	}

	return value, nil
}

// List returns all line/value pairs, starting from the beginning of the file (line 0 is first record).
func (s *Store) List() ([][2]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([][2]interface{}, 0, s.lineCount)
	_, err := s.file.Seek(0, io.SeekStart) // Always start at the beginning
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start: %v", err)
	}

	for lineNum := uint64(0); lineNum < s.lineCount; lineNum++ {
		var typeByte byte
		err = binary.Read(s.file, binary.LittleEndian, &typeByte)
		if err != nil {
			return nil, fmt.Errorf("failed to read type byte at line %d: %v", lineNum, err)
		}
		if typeByte != 0 {
			return nil, fmt.Errorf("invalid record type %d at line %d", typeByte, lineNum)
		}

		var valLen uint32
		err = binary.Read(s.file, binary.LittleEndian, &valLen)
		if err != nil {
			return nil, fmt.Errorf("failed to read value length at line %d: %v", lineNum, err)
		}
		if valLen > 1<<20 {
			return nil, fmt.Errorf("invalid value length %d at line %d", valLen, lineNum)
		}

		value := make([]byte, valLen)
		n, err := io.ReadFull(s.file, value)
		if err != nil {
			return nil, fmt.Errorf("failed to read value at line %d (read %d/%d bytes): %v", lineNum, n, valLen, err)
		}
		result = append(result, [2]interface{}{lineNum, value})
	}

	return result, nil
}

// ListAllReverse returns all line/value pairs, starting from the end of the file, with original line numbers.
func (s *Store) ListAllReverse() ([][2]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([][2]interface{}, 0, s.lineCount)
	if s.lineCount == 0 {
		return result, nil
	}

	for lineNum := s.lineCount - 1; ; lineNum-- {
		// Seek to the index entry for the current line
		indexOffset := int64(lineNum * 16) // 16 bytes per entry
		_, err := s.indexFile.Seek(indexOffset, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to index offset %d: %v", indexOffset, err)
		}

		indexEntry := make([]byte, 16)
		n, err := io.ReadFull(s.indexFile, indexEntry)
		if err != nil || n != 16 {
			return nil, fmt.Errorf("failed to read index entry for line %d: %v", lineNum, err)
		}

		dataOffset := binary.LittleEndian.Uint64(indexEntry[8:16])
		_, err = s.file.Seek(int64(dataOffset), io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to seek to data offset %d: %v", dataOffset, err)
		}

		var typeByte byte
		err = binary.Read(s.file, binary.LittleEndian, &typeByte)
		if err != nil {
			return nil, fmt.Errorf("failed to read type byte at line %d: %v", lineNum, err)
		}
		if typeByte != 0 {
			return nil, fmt.Errorf("invalid record type %d at line %d", typeByte, lineNum)
		}

		var valLen uint32
		err = binary.Read(s.file, binary.LittleEndian, &valLen)
		if err != nil {
			return nil, fmt.Errorf("failed to read value length at line %d: %v", lineNum, err)
		}
		if valLen > 1<<20 {
			return nil, fmt.Errorf("invalid value length %d at line %d", valLen, lineNum)
		}

		value := make([]byte, valLen)
		n, err = io.ReadFull(s.file, value)
		if err != nil {
			return nil, fmt.Errorf("failed to read value at line %d (read %d/%d bytes): %v", lineNum, n, valLen, err)
		}

		// Use the original lineNum as the ID
		result = append(result, [2]interface{}{lineNum, value})

		if lineNum == 0 {
			break
		}
	}

	return result, nil
}

// GetLastLine returns the line number of the last item in the store.
func (s *Store) GetLastLine() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.lineCount == 0 {
		return 0, fmt.Errorf("store is empty")
	}
	return s.lineCount - 1, nil
}

// Polish compacts the database by rewriting all values and updating the index.
func (s *Store) Polish() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	origPath := s.file.Name()
	backupPath := origPath + ".backup"
	err := s.backupTo(backupPath, false)
	if err != nil {
		return fmt.Errorf("failed to create backup before polish: %v", err)
	}

	tempPath := origPath + ".tmp"
	tempFile, err := os.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to create temp data file: %v", err)
	}
	defer tempFile.Close()

	tempIndexPath := origPath + ".idx.tmp"
	tempIndexFile, err := os.OpenFile(tempIndexPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to create temp index file: %v", err)
	}
	defer tempIndexFile.Close()

	_, err = s.file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to start: %v", err)
	}

	newLine := uint64(0)
	for i := uint64(0); i < s.lineCount; i++ {
		var typeByte byte
		err = binary.Read(s.file, binary.LittleEndian, &typeByte)
		if err != nil {
			return fmt.Errorf("failed to read type byte at line %d: %v", i, err)
		}
		if typeByte != 0 {
			return fmt.Errorf("invalid record type %d at line %d", typeByte, i)
		}

		var valLen uint32
		err = binary.Read(s.file, binary.LittleEndian, &valLen)
		if err != nil {
			return fmt.Errorf("failed to read value length at line %d: %v", i, err)
		}
		if valLen > 1<<20 {
			return fmt.Errorf("invalid value length %d at line %d", valLen, i)
		}

		value := make([]byte, valLen)
		n, err := io.ReadFull(s.file, value)
		if err != nil {
			return fmt.Errorf("failed to read value at line %d (read %d/%d bytes): %v", i, n, valLen, err)
		}

		record := make([]byte, 1+4+len(value))
		record[0] = 0
		binary.LittleEndian.PutUint32(record[1:5], valLen)
		copy(record[5:], value)

		dataOffset, err := tempFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get temp data offset: %v", err)
		}
		_, err = tempFile.Write(record)
		if err != nil {
			return fmt.Errorf("failed to write polished record: %v", err)
		}

		indexEntry := make([]byte, 16)
		binary.LittleEndian.PutUint64(indexEntry[0:8], newLine)
		binary.LittleEndian.PutUint64(indexEntry[8:16], uint64(dataOffset))
		_, err = tempIndexFile.Write(indexEntry)
		if err != nil {
			return fmt.Errorf("failed to write polished index entry: %v", err)
		}
		newLine++
	}

	err = tempFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync temp data file: %v", err)
	}
	err = tempIndexFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync temp index file: %v", err)
	}

	err = s.file.Close()
	if err != nil {
		return fmt.Errorf("failed to close original data file: %v", err)
	}
	err = s.indexFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close original index file: %v", err)
	}

	err = os.Rename(tempPath, origPath)
	if err != nil {
		return fmt.Errorf("failed to replace original data file: %v", err)
	}
	err = os.Rename(tempIndexPath, origPath+".idx")
	if err != nil {
		return fmt.Errorf("failed to replace original index file: %v", err)
	}

	s.file, err = os.OpenFile(origPath, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to reopen polished data file: %v", err)
	}
	s.indexFile, err = os.OpenFile(origPath+".idx", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		s.file.Close()
		return fmt.Errorf("failed to reopen polished index file: %v", err)
	}
	s.lineCount = newLine

	return nil
}

// Backup creates a backup of the database at the specified path.
func (s *Store) Backup(path string, polished bool) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.backupTo(path, polished)
}

// backupTo is a helper function to create a backup.
func (s *Store) backupTo(path string, polished bool) error {
	backupFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %v", err)
	}
	defer backupFile.Close()

	_, err = s.file.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to start: %v", err)
	}
	_, err = io.Copy(backupFile, s.file)
	if err != nil {
		return fmt.Errorf("failed to copy data file: %v", err)
	}

	err = backupFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync backup file: %v", err)
	}

	// Backup index file
	backupIndexPath := path + ".idx"
	backupIndexFile, err := os.OpenFile(backupIndexPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("failed to create backup index file: %v", err)
	}
	defer backupIndexFile.Close()

	_, err = s.indexFile.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to start of index file: %v", err)
	}
	_, err = io.Copy(backupIndexFile, s.indexFile)
	if err != nil {
		return fmt.Errorf("failed to copy index file: %v", err)
	}

	err = backupIndexFile.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync backup index file: %v", err)
	}

	return nil
}

// Close closes the store and releases resources.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.file.Close()
	if err != nil {
		s.indexFile.Close() // Try to close index file even if data file fails
		return fmt.Errorf("failed to close data file: %v", err)
	}
	err = s.indexFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close index file: %v", err)
	}
	return nil
}
