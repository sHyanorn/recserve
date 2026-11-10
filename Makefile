GO      ?= go
DATA    ?= data
ENCODER ?= mean
BUNDLE  ?=

.DEFAULT_GOAL := help
.PHONY: help build test test-real e2e lint fmt data bundle infra-up infra-down

help: ## Show available targets
	@awk -F':.*## ' '/^[a-z0-9-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build all commands into bin/
	$(GO) build -o bin/ ./cmd/...

test: ## Run all tests with the race detector
	$(GO) test -race ./...

test-real: ## Run internal/recommend on the real data (make data) and BUNDLE, a directory made by make bundle; with the race detector it runs past go test's 10 minutes
	RECSERVE_DATA=$(abspath $(DATA)) RECSERVE_BUNDLE=$(abspath $(BUNDLE)) $(GO) test -race -count=1 -timeout 50m ./internal/recommend/

e2e: ## Run the binaries against Kafka and Valkey (needs infra-up); pauses Valkey and stops Kafka, so run nothing else on them
	RECSERVE_E2E=1 RECSERVE_VALKEY_ADDR=localhost:6379 RECSERVE_KAFKA_BROKERS=localhost:9094 $(GO) test -race -count=1 ./internal/e2e/

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
