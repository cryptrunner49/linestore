# DOCS.md

## StoneKVR - A Simple Key/Value Store Library

`StoneKVR` is a lightweight, persistent key/value store written in Go. It provides a simple API for storing, retrieving, and deleting key/value pairs on disk, with additional features like database polishing (compaction) and backups. This library is ideal for projects requiring a minimalistic, embedded key/value database with concurrent access support.

### Features

- Persistent storage on disk
- In-memory index for fast lookups
- Thread-safe operations with `sync.RWMutex`
- Support for full and polished backups
- Database compaction with `Polish()`
- Simple and intuitive API

### Project Structure

```text
stonekv/
├── cmd/
│   └── main.go         # Example usage of the library
├── stone/
│   ├── stone.go        # Core library implementation
│   └── stone_test.go   # Unit tests for the library
└── DOCS.md             # This documentation
```

---

## Installation

To use `StoneKVR` in your project, follow these steps:

1. **Add the library to your project:**
   Since this is a custom library, you can either:
   - Copy the `stone` directory into your project, or
   - Host it in a repository (e.g., GitHub) and import it.

   For example, if hosted at `github.com/cryptrunner49/stonekv`, run:

   ```bash
   go get github.com/cryptrunner49/stonekv/stone
   ```

2. **Import the library in your Go code:**

   ```go
   import "github.com/cryptrunner49/stonekv/stone"
   ```

3. **Ensure your Go module is initialized:**
   If your project uses Go modules, run:

   ```bash
   go mod init yourmodule
   go mod tidy
   ```

---

## Quick Start

Here’s a basic example to get you started with `StoneKVR`:

```go
package main

import (
 "fmt"
 "log"
 "github.com/cryptrunner49/stonekv/stone"
)

func main() {
 // Initialize a new store
 store, err := stone.NewStore("mystore.db")
 if err != nil {
  log.Fatal(err)
 }
 defer store.Close() // Always close the store when done

 // Set a key/value pair
 err = store.Set([]byte("name"), []byte("Alice"))
 if err != nil {
  log.Fatal(err)
 }

 // Get the value
 value, err := store.Get([]byte("name"))
 if err != nil {
  log.Fatal(err)
 }
 fmt.Println("Value:", string(value)) // Output: Value: Alice

 // Delete the key
 err = store.Delete([]byte("name"))
 if err != nil {
  log.Fatal(err)
 }
}
```

This example creates a store, sets a key/value pair, retrieves it, and then deletes it. The data is persisted to `mystore.db`.

---

## Usage Examples

### Setting and Retrieving Values

```go
store, _ := stone.NewStore("data.db")
defer store.Close()

// Set multiple key/value pairs
store.Set([]byte("user1"), []byte("Bob"))
store.Set([]byte("user2"), []byte("Charlie"))

// Retrieve values
val1, _ := store.Get([]byte("user1"))
val2, _ := store.Get([]byte("user2"))
fmt.Println(string(val1)) // Output: Bob
fmt.Println(string(val2)) // Output: Charlie
```

### Deleting a Key

```go
store, _ := stone.NewStore("data.db")
defer store.Close()

store.Set([]byte("temp"), []byte("delete me"))
store.Delete([]byte("temp"))

val, err := store.Get([]byte("temp"))
if err != nil {
 fmt.Println("Key not found, as expected:", err)
}
```

### Creating Backups

```go
store, _ := stone.NewStore("data.db")
defer store.Close()

// Full backup (includes all records, even deleted ones)
store.Backup("full_backup.db", false)
fmt.Println("Full backup created")

// Polished backup (only active key/value pairs)
store.Backup("polished_backup.db", true)
fmt.Println("Polished backup created")
```

### Polishing the Database

```go
store, _ := stone.NewStore("data.db")
defer store.Close()

store.Set([]byte("key1"), []byte("value1"))
store.Delete([]byte("key1")) // Mark key1 as deleted
store.Polish()               // Compact the database
fmt.Println("Database polished")
```

---

## API Documentation

### `type Store`

The core struct representing the key/value store.

- **Fields (not exported):**
  - `file`: The underlying file handle.
  - `index`: An in-memory map of keys to value offsets.
  - `mu`: A read/write mutex for thread safety.

### `func NewStore(path string) (*Store, error)`

Creates or opens a store at the specified file path.

- **Parameters:**
  - `path` (string): The file path for the database (e.g., `"data.db"`).
- **Returns:**
  - `*Store`: A pointer to the initialized store.
  - `error`: An error if the file cannot be opened or the index cannot be built.
- **Example:**

  ```go
  store, err := stone.NewStore("mydb.db")
  if err != nil {
      log.Fatal(err)
  }
  defer store.Close()
  ```

### `func (s *Store) Set(key, value []byte) error`

Stores a key/value pair in the database.

- **Parameters:**
  - `key` ([]byte): The key to store.
  - `value` ([]byte): The value to associate with the key.
- **Returns:**
  - `error`: An error if the write fails.
- **Example:**

  ```go
  err := store.Set([]byte("id"), []byte("12345"))
  ```

### `func (s *Store) Get(key []byte) ([]byte, error)`

Retrieves the value associated with a key.

- **Parameters:**
  - `key` ([]byte): The key to look up.
- **Returns:**
  - `[]byte`: The value, if found.
  - `error`: An error if the key is not found or reading fails.
- **Example:**

  ```go
  value, err := store.Get([]byte("id"))
  if err == nil {
      fmt.Println(string(value)) // Output: 12345
  }
  ```

### `func (s *Store) Delete(key []byte) error`

Marks a key as deleted in the database.

- **Parameters:**
  - `key` ([]byte): The key to delete.
- **Returns:**
  - `error`: An error if the write fails.
- **Example:**

  ```go
  err := store.Delete([]byte("id"))
  ```

### `func (s *Store) Polish() error`

Compacts the database by rewriting only active key/value pairs, removing deleted records.

- **Returns:**
  - `error`: An error if the operation fails.
- **Notes:**
  - Creates a backup of the original file (`.backup` suffix) before polishing.
- **Example:**

  ```go
  err := store.Polish()
  if err == nil {
      fmt.Println("Database compacted")
  }
  ```

### `func (s *Store) Backup(path string, polished bool) error`

Creates a backup of the database at the specified path.

- **Parameters:**
  - `path` (string): The file path for the backup.
  - `polished` (bool): If `true`, only active key/value pairs are backed up; if `false`, a full copy is made.
- **Returns:**
  - `error`: An error if the backup fails.
- **Example:**

  ```go
  store.Backup("backup.db", true) // Polished backup
  ```

### `func (s *Store) Close() error`

Closes the store and releases resources.

- **Returns:**
  - `error`: An error if closing the file fails.
- **Example:**

  ```go
  err := store.Close()
  ```

---

## Testing

The library includes comprehensive unit tests in `stone/stone_test.go`. To run the tests:

```bash
cd stone
go test -v
```

The tests cover:

- Basic CRUD operations (`Set`, `Get`, `Delete`)
- Persistence across store reopenings
- Database polishing
- Full and polished backups

---

## Notes

- **Thread Safety:** All public methods (`Set`, `Get`, `Delete`, `Polish`, `Backup`, `Close`) are thread-safe due to the use of `sync.RWMutex`.
- **Persistence:** Data is written to disk immediately, but deleted keys remain in the file until `Polish()` is called.
- **Error Handling:** Always check returned errors to ensure operations succeed.

---

## Contributing

Feel free to fork the repository, submit issues, or create pull requests. Contributions to improve performance, add features, or enhance documentation are welcome!

---

## License

This library is provided as-is with no specific license attached. Define your own licensing terms if you distribute it further.
