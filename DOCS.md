# StoneKV Library Documentation

## Overview

**StoneKV** is a lightweight, persistent key-value store written in Go. It provides a simple API for storing, retrieving, and deleting key-value pairs on disk, with support for concurrent access, backups, and database compaction (polishing). This library is ideal for applications needing a simple, file-based persistence layer without the overhead of a full database system.

This document explains how to use the `stone` library, including setup, basic usage, and a complete API reference.

---

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [API Reference](#api-reference)
   - [NewStore](#newstore)
   - [Set](#set)
   - [Get](#get)
   - [Delete](#delete)
   - [Polish](#polish)
   - [Backup](#backup)
   - [Close](#close)
4. [Example Usage](#example-usage)
5. [Testing](#testing)
6. [Contributing](#contributing)
7. [License](#license)

---

## Installation

To use the `stone` library in your Go project:

1. Ensure you have Go installed (version 1.16 or later recommended).
2. Add the library to your project:

   ```bash
   go get github.com/cryptrunner49/stonekv/stone
   ```

3. Import the library in your code:

   ```go
   import "github.com/cryptrunner49/stonekv/stone"
   ```

The library has no external dependencies beyond the Go standard library, making it easy to integrate.

---

## Quick Start

Here's a simple example to get you started with StoneKV:

```go
package main

import (
    "fmt"
    "log"
    "github.com/cryptrunner49/stonekv/stone"
)

func main() {
    // Open or create a store
    store, err := stone.NewStore("mystore.db")
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Set a key-value pair
    err = store.Set([]byte("name"), []byte("Alice"))
    if err != nil {
        log.Fatal(err)
    }

    // Retrieve the value
    value, err := store.Get([]byte("name"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Value:", string(value)) // Output: Value: Alice
}
```

This creates a database file `mystore.db`, stores a key-value pair, and retrieves it.

---

## API Reference

The `stone` package provides the following methods on the `Store` type. All methods are thread-safe due to internal mutex locking.

### NewStore

```go
func NewStore(path string) (*Store, error)
```

Initializes or opens a StoneKV store at the specified file path. If the file doesn’t exist, it creates one. On startup, it builds an in-memory index of existing key-value pairs.

- **Parameters**:
  - `path` (string): Path to the database file (e.g., `"data.db"`).
- **Returns**:
  - `*Store`: A pointer to the initialized store.
  - `error`: Non-nil if the file cannot be opened or the index cannot be built.

**Example**:

```go
store, err := stone.NewStore("data.db")
if err != nil {
    log.Fatal(err)
}
defer store.Close()
```

---

### Set

```go
func (s *Store) Set(key, value []byte) error
```

Stores a key-value pair in the database. Overwrites the value if the key already exists.

- **Parameters**:
  - `key` ([]byte): The key to store.
  - `value` ([]byte): The value to associate with the key.
- **Returns**:
  - `error`: Non-nil if the write operation fails.

**Example**:

```go
err := store.Set([]byte("user"), []byte("Bob"))
if err != nil {
    log.Fatal(err)
}
```

---

### Get

```go
func (s *Store) Get(key []byte) ([]byte, error)
```

Retrieves the value associated with a key.

- **Parameters**:
  - `key` ([]byte): The key to look up.
- **Returns**:
  - `[]byte`: The value associated with the key.
  - `error`: Non-nil if the key is not found or reading fails.

**Example**:

```go
value, err := store.Get([]byte("user"))
if err != nil {
    log.Fatal(err)
}
fmt.Println(string(value)) // Output: Bob
```

---

### Delete

```go
func (s *Store) Delete(key []byte) error
```

Removes a key and its associated value from the database. The deletion is logged to disk, and the key is removed from the in-memory index.

- **Parameters**:
  - `key` ([]byte): The key to delete.
- **Returns**:
  - `error`: Non-nil if the write operation fails.

**Example**:

```go
err := store.Delete([]byte("user"))
if err != nil {
    log.Fatal(err)
}
```

---

### Polish

```go
func (s *Store) Polish() error
```

Compacts the database by creating a new file containing only active key-value pairs, removing deleted or overwritten entries. The original file is backed up before replacement.

- **Returns**:
  - `error`: Non-nil if the operation fails (e.g., file I/O errors).

**Example**:

```go
err := store.Polish()
if err != nil {
    log.Fatal(err)
}
fmt.Println("Database compacted")
```

---

### Backup

```go
func (s *Store) Backup(path string, polished bool) error
```

Creates a backup of the database at the specified path. If `polished` is `true`, only active key-value pairs are included; otherwise, it’s a full copy of the file.

- **Parameters**:
  - `path` (string): Path to the backup file.
  - `polished` (bool): If true, creates a compact backup; if false, copies the entire file.
- **Returns**:
  - `error`: Non-nil if the backup fails.

**Example**:

```go
// Full backup
err := store.Backup("backup.db", false)
if err != nil {
    log.Fatal(err)
}

// Polished backup
err = store.Backup("polished_backup.db", true)
if err != nil {
    log.Fatal(err)
}
```

---

### Close

```go
func (s *Store) Close() error
```

Closes the store and releases file resources. Always call this when done using the store to avoid resource leaks.

- **Returns**:
  - `error`: Non-nil if closing the file fails.

**Example**:

```go
err := store.Close()
if err != nil {
    log.Fatal(err)
}
```

---

## Example Usage

The following example demonstrates a complete workflow using the `stone` library:

```go
package main

import (
    "fmt"
    "log"
    "github.com/cryptrunner49/stonekv/stone"
)

func main() {
    // Initialize the store
    store, err := stone.NewStore("example.db")
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Set key-value pairs
    store.Set([]byte("key1"), []byte("value1"))
    store.Set([]byte("key2"), []byte("value2"))

    // Retrieve a value
    val, _ := store.Get([]byte("key1"))
    fmt.Println("key1:", string(val)) // Output: key1: value1

    // Delete a key
    store.Delete([]byte("key1"))

    // Backup the database
    store.Backup("example_backup.db", false)
    fmt.Println("Backup created")

    // Polish the database
    store.Polish()
    fmt.Println("Database polished")

    // Verify deletion
    _, err = store.Get([]byte("key1"))
    if err != nil {
        fmt.Println("key1 was deleted as expected")
    }
}
```

---

## Testing

The library includes a comprehensive test suite in `stone/stone_test.go`. To run the tests:

```bash
cd stone
go test -v
```

The tests cover:

- Basic CRUD operations (`Set`, `Get`, `Delete`).
- Persistence across store reopenings.
- Database polishing (`Polish`).
- Full and polished backups (`Backup`).

---

## Contributing

Contributions are welcome! To contribute:

1. Fork the repository at `github.com/cryptrunner49/stonekv`.
2. Create a feature branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -m "Add feature"`).
4. Push to the branch (`git push origin feature-name`).
5. Open a pull request.

Please include tests for new features and ensure existing tests pass.

---

## License

This library is licensed under the MIT License. See the `LICENSE` file in the repository for details.
