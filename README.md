# StoneKV 🪨🚀

**StoneKV** is an **embedded NoSQL key/value store** written in Go. It provides on‑disk persistence, making it a lightweight and efficient solution for modern applications.

## Overview 🔍

StoneKV offers:

- **Embedded storage** for quick local data persistence.
- **On‑disk durability** ensuring your data stays safe.
- **Native Go integration** for fast and concurrent operations.

## Features ✨

- **Lightweight & Fast**: Optimized for performance in resource‑constrained environments.
- **Simple API**: Easy-to‑use methods to store, retrieve, and query data.
- **On‑Disk Persistence**: Reliable storage without the need for a separate server.
- **Concurrent Access**: Built with Go’s concurrency in mind.

## Quick Start 🚀

1. **Install via Go Modules:**

   ```bash
   go get github.com/cryptrunner49/stonekv
   ```  

2. **Basic Usage Example:**

   ```go
   package main  

   import (
       "fmt"
       "log"

       "github.com/cryptrunner49/stonekv/stone"
   )

   func main() {
       // Initialize the store (creates a new one if it doesn't exist)
       store, err := stonekv.NewStore("data.db")
       if err != nil {
           log.Fatal(err)
       }
       defer store.Close()

       // Set a key/value pair
       err = store.Set("greeting", "Hello, StoneKV! 👋")
       if err != nil {
           log.Fatal(err)
       }

       // Retrieve a value
       value, err := store.Get("greeting")
       if err != nil {
           log.Fatal(err)
       }
       fmt.Println(value)
   }
   ```

### Examples of Storing Different Data Types

1. **Strings**:

   ```go
   store.Set([]byte("name"), []byte("Alice"))
   ```

   - Simple and direct—strings are already byte-compatible in Go.

2. **Integers**:

   ```go
   num := 42
   buf := make([]byte, 8)
   binary.LittleEndian.PutUint64(buf, uint64(num))
   store.Set([]byte("age"), buf)
   ```

   - Convert the integer to bytes using Go’s `binary` package.

3. **Structs** (using JSON serialization):

   ```go
   type User struct {
       Name string
       Age  int
   }
   user := User{"Alice", 30}
   userBytes, _ := json.Marshal(user)
   store.Set([]byte("user:1"), userBytes)
   ```

   - Serialize the struct to bytes with JSON (or another method like Gob).

4. **Images or Blobs**:

   ```go
   imageData, _ := os.ReadFile("image.png")
   store.Set([]byte("image:profile"), imageData)
   ```

   - Load the file as raw bytes and store it directly.

When retrieving data, you’d reverse the process:

- For a string: Cast the `[]byte` back to a string.
- For an integer: Use `binary.LittleEndian.Uint64()` to decode the bytes.
- For a struct: Use `json.Unmarshal()` to reconstruct it.
- For an image: Write the bytes back to a file or process them directly.

## Contributing 🤝

We welcome contributions! Check out our [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## License 📜

Distributed under the MIT License. See [LICENSE](./LICENSE) for more information.
