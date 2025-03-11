# Variables
BINARY_NAME = linestore
BIN_DIR = bin
SRC_DIR = cmd
PACKAGE = ./...

# Default target
.PHONY: all
all: build

# Build the executable and place it in the bin directory
.PHONY: build
build:
	@mkdir -p $(BIN_DIR)
	@go build -o $(BIN_DIR)/$(BINARY_NAME) $(SRC_DIR)/main.go
	@echo "✅ Build complete! Binary located at $(BIN_DIR)/$(BINARY_NAME)"

# Run the executable from the bin directory
.PHONY: run
run: build
	@echo "🚀 Running $(BINARY_NAME)..."
	@$(BIN_DIR)/$(BINARY_NAME)
	@echo "🏁 Execution finished!"

# Test all packages
.PHONY: test
test:
	@echo "🧪 Running tests..."
	@go test -v $(PACKAGE)
	@echo "✅ Tests completed!"

# Build and run the executable
.PHONY: build-run
build-run: build
	@echo "🚀 Running $(BINARY_NAME)..."
	@$(BIN_DIR)/$(BINARY_NAME)
	@echo "🏁 Execution finished!"

# Build, test, and run the executable
.PHONY: build-test-run
build-test-run: build test
	@echo "🚀 Running $(BINARY_NAME)..."
	@$(BIN_DIR)/$(BINARY_NAME)
	@echo "🏁 Execution finished!"

# Clean up the bin directory
.PHONY: clean
clean:
	@echo "🧹 Cleaning up..."
	@rm -rf $(BIN_DIR)
	@echo "✅ Cleanup complete!"

# Help message
.PHONY: help
help:
	@echo "📜 Makefile targets:"
	@echo "  make         - Build the executable (default)"
	@echo "  make build   - Build the executable into bin/ ✅"
	@echo "  make run     - Build and run the executable 🚀"
	@echo "  make test    - Run all tests 🧪"
	@echo "  make build-run - Build and run the executable 🚀"
	@echo "  make build-test-run - Build, test, and run the executable 🚀🧪"
	@echo "  make clean   - Remove the bin directory 🧹"
	@echo "  make help    - Show this help message 📜"