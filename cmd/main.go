package main

import (
	"fmt"
	"log"

	"github.com/cryptrunner49/stonekv/stone"
)

func main() {
	// Initialize the store
	store, err := stone.NewStore("data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	// Set some key/value pairs
	err = store.Set([]byte("greeting"), []byte("Hello, StoneKVR! 👋"))
	if err != nil {
		log.Fatal(err)
	}
	err = store.Set([]byte("farewell"), []byte("Goodbye!"))
	if err != nil {
		log.Fatal(err)
	}

	// Delete a key
	err = store.Delete([]byte("farewell"))
	if err != nil {
		log.Fatal(err)
	}

	// Retrieve a value
	value, err := store.Get([]byte("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(value)) // Outputs: Hello, StoneKVR! 👋

	// Create a full backup
	err = store.Backup("data_full_backup.db", false)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Full backup created at data_full_backup.db")

	// Create a polished backup
	err = store.Backup("data_polished_backup.db", true)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Polished backup created at data_polished_backup.db")

	// Polish the database
	err = store.Polish()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Database polished")
}