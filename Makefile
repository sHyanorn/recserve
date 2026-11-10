GO      ?= go
DATA    ?= data
ENCODER ?= mean

.DEFAULT_GOAL := help
.PHONY: help build test lint fmt data bundle infra-up infra-down

help: ## Show available targets
	@awk -F':.*## ' '/^[a-z0-9-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

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

bundle: ## Build the baseline bundle into DATA/bundle (needs uv); ENCODER=onnx adds user_encoder.onnx
	cd tools/bundle && uv run --locked python build.py $(abspath $(DATA)) $(abspath $(DATA))/bundle --encoder $(ENCODER)

infra-up: ## Start Kafka and Valkey (deploy/compose.yaml) and wait until they are healthy
	docker compose -f deploy/compose.yaml up -d --wait

infra-down: ## Stop Kafka and Valkey and delete their data
	docker compose -f deploy/compose.yaml down -v
