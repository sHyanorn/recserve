GO      ?= go
DATA    ?= data
ENCODER ?= mean
BUNDLE  ?=
TAG     ?= dev
VERSION ?=
COMPOSE := docker compose -f deploy/compose.yaml

.DEFAULT_GOAL := help
BENCH   := DATA=$(abspath $(DATA)) VERSION=$(VERSION)
.PHONY: bench-ref bench-sweep bench-profile bench-compare bench-fault bench-hedge bench-swap bench-kind bench-vegeta
.PHONY: help build test test-real e2e lint fmt proto data bundle images infra-up infra-down up down test-alerts kind-up kind-swap kind-check kind-netpol kind-down

help: ## Show available targets
	@awk -F':.*## ' '/^[a-z0-9-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build all commands into bin/
	$(GO) build -o bin/ ./cmd/...

test: ## Run all tests with the race detector
	$(GO) test -race ./...

test-real: ## Run internal/recommend on the real data (make data) and BUNDLE, a directory made by make bundle; with the race detector it runs past go test's 10 minutes
	RECSERVE_DATA=$(abspath $(DATA)) RECSERVE_BUNDLE=$(abspath $(BUNDLE)) $(GO) test -race -count=1 -timeout 50m ./internal/recommend/

e2e: ## Run the binaries, most tests against Kafka and Valkey (needs infra-up); pauses Valkey and stops Kafka, so run nothing else on them
	RECSERVE_E2E=1 RECSERVE_VALKEY_ADDR=localhost:6379 RECSERVE_KAFKA_BROKERS=localhost:9094 $(GO) test -race -count=1 ./internal/e2e/

lint: ## Run golangci-lint
	golangci-lint run ./...

fmt: ## Format code
	golangci-lint fmt ./...

proto: ## Lint the proto files and regenerate their Go code (needs buf v1.75.0)
	buf lint
	buf generate

data: ## Download the Amazon 2023 Video_Games split and metadata into DATA
	scripts/data.sh $(DATA)

bundle: ## Build the baseline bundle into DATA/bundle (needs uv); ENCODER=onnx adds user_encoder.onnx, RANKER=1 a trained ranker.onnx
	cd tools/bundle && uv run --locked python build.py $(abspath $(DATA)) $(abspath $(DATA))/bundle --encoder $(ENCODER) $(if $(RANKER),--ranker)

images: ## Build the gateway, annshard and ingest images as recserve/<name>:TAG; DOCKER_BUILD_FLAGS adds docker build flags
	for t in gateway annshard ingest; do docker build $(DOCKER_BUILD_FLAGS) --target $$t -t recserve/$$t:$(TAG) . || exit 1; done

infra-up: ## Start Kafka and Valkey (deploy/compose.yaml) and wait until they are healthy
	$(COMPOSE) up -d --wait

infra-down: ## Stop Kafka and Valkey and delete their data
	$(COMPOSE) down -v

up: ## Start the service in Docker on the images of make images: DATA, and VERSION, the bundle version to serve
	DATA=$(abspath $(DATA)) VERSION=$(VERSION) TAG=$(TAG) $(COMPOSE) --profile app up -d --wait

down: ## Stop what make up started and delete its data
	$(COMPOSE) --profile app down -v

test-alerts: ## Unit-test the Prometheus alert rules with promtool
	$(COMPOSE) run --rm --no-deps --entrypoint promtool -w /etc/prometheus prometheus test rules alerts_test.yml

kind-up: ## Create the kind cluster "recserve" and deploy deploy/kind on the images of make images and DATA's bundles (needs kind, kubectl)
	scripts/kind-up.sh $(DATA)

kind-swap: ## Swap the gateways in kind to bundle VERSION, once the previous swap has ended
	scripts/kind-swap.sh $(VERSION)

kind-check: ## Under load in kind, roll the gateways, then the replicas, then swap to VERSION: no error and nothing degraded
	scripts/kind-check.sh $(DATA) $(VERSION)

kind-netpol: ## Check in kind that only the gateways reach the replicas' port; fails where NetworkPolicy is not enforced
	scripts/kind-netpol.sh

kind-down: ## Delete the kind cluster
	kind delete cluster --name recserve

bench-ref: build ## Take the session's noise reference on an idle machine (docs/BENCHMARKS.md), before the bench- targets
	mkdir -p bench-results/m6 && taskset -c 1 bin/noisebase -n 10 -cpu 1 -out bench-results/m6/noise-reference.json

bench-sweep: build ## Load sweep in compose (docs/BENCHMARKS.md): SCENARIO replay, writes or repeats, CACHE on or off, RATES (empty: the grid, to GRID_MAX if set); DATA, VERSION
	$(BENCH) GRID_MAX=$(GRID_MAX) scripts/bench-sweep.sh $(SCENARIO) $(CACHE) $(RATES)

bench-fault: build ## Fault runs in compose (docs/BENCHMARKS.md): FAULT F1 to F6, RATE half the headline rate; DATA, VERSION
	$(BENCH) scripts/bench-fault.sh $(FAULT) $(RATE)

bench-hedge: build ## The hedge delay's default from shard call latencies in compose (docs/BENCHMARKS.md): RATE half the headline rate; DATA, VERSION
	$(BENCH) scripts/bench-hedge.sh $(RATE)

bench-swap: build ## Bundle swaps under load in compose, with GC on and off (docs/BENCHMARKS.md): RATE half the headline rate; DATA, VERSION
	$(BENCH) scripts/bench-swap.sh $(RATE)

bench-kind: build ## kind-check's rollouts and swap to VERSION with SPEC §5's budgets, three new clusters (docs/BENCHMARKS.md); DATA, VERSION
	$(BENCH) scripts/bench-kind.sh

bench-vegeta: build ## loadgen and vegeta at RATE, half the headline rate, three runs each (docs/BENCHMARKS.md); needs vegeta v12.12.0; DATA, VERSION
	$(BENCH) scripts/bench-vegeta.sh $(RATE)

bench-profile: build ## CPU profiles of the gateway and a replica under load in compose (docs/BENCHMARKS.md); DATA, VERSION
	$(BENCH) scripts/bench-profile.sh

bench-compare: build ## Before and after an optimization: BEFORE, a commit; vec's benchmark and the service at RATES (200 400); DATA, VERSION, DOCKER_BUILD_FLAGS
	$(BENCH) DOCKER_BUILD_FLAGS="$(DOCKER_BUILD_FLAGS)" scripts/bench-compare.sh $(BEFORE)
