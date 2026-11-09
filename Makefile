GO   ?= go
DATA ?= data

.DEFAULT_GOAL := help
.PHONY: help build test lint fmt data bundle

help: ## Show available targets
	@awk -F':.*## ' '/^[a-z0-9]+:.*## / {printf "  %-8s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build all commands into bin/
	$(GO) build -o bin/ ./cmd/...

test: ## Run all tests with the race detector
	$(GO) test -race ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

fmt: ## Format code
	golangci-lint fmt ./...

data: ## Download the Amazon 2023 Video_Games split and metadata into DATA
	scripts/data.sh $(DATA)

bundle: ## Build the baseline bundle into DATA/bundle (needs uv)
	cd tools/bundle && uv run python build.py $(abspath $(DATA)) $(abspath $(DATA))/bundle
