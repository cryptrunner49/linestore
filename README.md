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

       "github.com/cryptrunner49/stonekv"
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

## Contributing 🤝

We welcome contributions! Check out our [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## License 📜

Distributed under the MIT License. See [LICENSE](./LICENSE) for more information.
