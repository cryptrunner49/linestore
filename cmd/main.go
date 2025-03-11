package main

import (
	"fmt"
	"log"

	"github.com/cryptrunner49/linestore/store"
)

func main() {
	store, err := store.NewStore("linestore.db")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	// Set some values
	line1, err := store.Set([]byte("Hello, Line Store! 👋"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = store.Set([]byte("Goodbye!"))
	if err != nil {
		log.Fatal(err)
	}

	// List from beginning
	fmt.Println("List from beginning:")
	pairs, err := store.List()
	if err != nil {
		log.Fatal(err)
	}
	for _, pair := range pairs {
		line := pair[0].(uint64)
		val := pair[1].([]byte)
		fmt.Printf("Line %d: %s\n", line, string(val))
	}

	// List from end!
	fmt.Println("\nList from end!")
	reversePairs, err := store.ListAllReverse()
	if err != nil {
		log.Fatal(err)
	}
	for _, pair := range reversePairs {
		line := pair[0].(uint64)
		val := pair[1].([]byte)
		fmt.Printf("Line %d: %s\n", line, string(val))
	}

	// Get last line number
	lastLine, err := store.GetLastLine()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nLast line number:", lastLine)

	// Retrieve a value
	value, err := store.Get(line1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("\nGet line", line1, ":", string(value))
}
