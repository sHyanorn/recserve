GO      ?= go
DATA    ?= data
CATEGORY ?= Video_Games
ENCODER ?= mean
BUNDLE  ?=
TAG     ?= dev
VERSION ?=
REF_FILE ?= bench-results/m7/noise-reference.json
COMPOSE := docker compose -f deploy/compose.yaml

.DEFAULT_GOAL := help
BENCH   := DATA=$(abspath $(DATA)) VERSION=$(VERSION) REF_FILE=$(REF_FILE)
.PHONY: bench-ref bench-sweep bench-profile bench-compare bench-fault bench-hedge bench-swap bench-kind bench-vegeta bench-ann bench-quality bench-bundles bench-exact bench-als-grid valid-reference bench-windows rebuilds bench-read bench-stream report
.PHONY: help build test test-real e2e lint fmt proto data bundle images infra-up infra-down up down test-alerts kind-up kind-swap kind-check kind-netpol kind-down

help: ## Show available targets
	@awk -F':.*## ' '/^[a-z0-9-]+:.*## / {printf "  %-10s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

build: ## Build all commands into bin/
	$(GO) build -o bin/ ./cmd/...

test: ## Run all tests with the race detector
	$(GO) test -race ./...

test-real: ## Run internal/recommend on the real data (make data) and BUNDLE, a directory made by make bundle, but TestRealDataQuality, a registered test read; with the race detector it runs past go test's 10 minutes
	@test -n "$(BUNDLE)" || { echo "set BUNDLE"; exit 1; }
	RECSERVE_DATA=$(abspath $(DATA)) RECSERVE_BUNDLE=$(abspath $(BUNDLE)) $(GO) test -race -count=1 -timeout 50m -skip '^TestRealDataQuality$$' ./internal/recommend/

e2e: ## Run the binaries, most tests against Kafka and Valkey (needs infra-up); pauses Valkey and stops Kafka, so run nothing else on them
	RECSERVE_E2E=1 RECSERVE_VALKEY_ADDR=localhost:6379 RECSERVE_KAFKA_BROKERS=localhost:9094 $(GO) test -race -count=1 ./internal/e2e/

lint: ## Run golangci-lint
	golangci-lint run ./...

fmt: ## Format code
	golangci-lint fmt ./...

proto: ## Lint the proto files and regenerate their Go code (needs buf v1.75.0)
	buf lint
	buf generate

data: ## Download an Amazon 2023 category's split into DATA: CATEGORY, Video_Games (with its metadata) by default
	scripts/data.sh $(DATA) $(CATEGORY)

bundle: ## Build CATEGORY's bundle into DATA/bundle (needs uv); ENCODER=onnx adds user_encoder.onnx, RANKER=1 a trained ranker.onnx, STREAM=DAYS with RANKER=1 a stream window and a ranker trained on the stream state (RECENCY=1: with the recency features), BASELINE=1 b7ff0634dd7b's ALS settings
	cd tools/bundle && uv run --locked python build.py $(abspath $(DATA)) $(abspath $(DATA))/bundle --category $(CATEGORY) --encoder $(ENCODER) $(if $(RANKER),--ranker) $(if $(STREAM),--stream $(STREAM)) $(if $(RECENCY),--recency) $(if $(BASELINE),--baseline)

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
	mkdir -p $(dir $(REF_FILE)) && taskset -c 1 bin/noisebase -n 10 -cpu 1 -out $(REF_FILE)

bench-sweep: build ## Load sweep in compose (docs/BENCHMARKS.md): SCENARIO replay, writes or repeats, CACHE on or off, RATES (empty: the grid, to GRID_MAX if set); DATA, VERSION
	$(BENCH) GRID_MAX=$(GRID_MAX) scripts/bench-sweep.sh $(SCENARIO) $(CACHE) $(RATES)

bench-fault: build ## Fault runs in compose (docs/BENCHMARKS.md): FAULT F1 to F6 (cache off), or none for the breaker's runs (cache on), RATE half the headline rate; DATA, VERSION
	$(BENCH) scripts/bench-fault.sh $(FAULT) $(RATE)

bench-hedge: build ## The hedge delay's default from shard call latencies in compose (docs/BENCHMARKS.md): RATE half the headline rate; DATA, VERSION
	$(BENCH) scripts/bench-hedge.sh $(RATE)

bench-swap: build ## Bundle swaps under load in compose, with GC on and off (docs/BENCHMARKS.md): RATE half the headline rate; DATA, VERSION
	$(BENCH) scripts/bench-swap.sh $(RATE)

bench-kind: build ## kind-check's rollouts and swap to VERSION with SPEC §5's budgets, three new clusters (docs/BENCHMARKS.md); DATA, VERSION
	$(BENCH) scripts/bench-kind.sh

bench-vegeta: build ## loadgen and vegeta at RATE, half the headline rate, three runs each (docs/BENCHMARKS.md); needs vegeta v12.12.0; DATA, VERSION
	$(BENCH) scripts/bench-vegeta.sh $(RATE)

bench-ann: build ## ANN in docs/BENCHMARKS.md, STEP profile or table: annbench and faiss on glove and synth in ANN_DATA (DATA/ann); PY, a Python with faiss-cpu 1.15.1, h5py and numpy
	$(BENCH) ANN_DATA=$(ANN_DATA) PY=$(PY) scripts/bench-ann.sh $(STEP)

bench-quality: ## Recommendation quality (docs/BENCHMARKS.md) into bench-results/m7/quality.md: TestRealDataQuality on DATA and the bundle DATA/bundle/VERSION; RECSERVE_ORT_LIB for its ONNX models
	mkdir -p bench-results/m7
	RECSERVE_DATA=$(abspath $(DATA)) RECSERVE_BUNDLE=$(abspath $(DATA))/bundle/$(VERSION) RECSERVE_QUALITY_OUT=$(abspath bench-results/m7/quality.md) \
		$(GO) test -count=1 -timeout 60m -run '^TestRealDataQuality$$' ./internal/recommend/

bench-bundles: ## A bundle against the baseline (docs/BENCHMARKS.md, seqrec-lab's bundle against the baseline) into bench-results/m7/bundles-<B's directory name>.md, a new file: TestRealDataQuality on DATA, the baseline DATA/bundle/VERSION and BUNDLE_B, a bundle directory; RECSERVE_ORT_LIB for their ONNX models
	@test -n "$(VERSION)" -a -n "$(BUNDLE_B)" || { echo "set VERSION and BUNDLE_B"; exit 1; }
	@test ! -e bench-results/m7/bundles-$(notdir $(abspath $(BUNDLE_B))).md || { echo "bench-results/m7/bundles-$(notdir $(abspath $(BUNDLE_B))).md exists"; exit 1; }
	RECSERVE_DATA=$(abspath $(DATA)) RECSERVE_BUNDLE=$(abspath $(DATA))/bundle/$(VERSION) RECSERVE_BUNDLE_B=$(abspath $(BUNDLE_B)) \
		RECSERVE_COMPARE_OUT=$(abspath bench-results/m7)/bundles-$(notdir $(abspath $(BUNDLE_B))).md \
		$(GO) test -count=1 -timeout 60m -run '^TestRealDataQuality$$' ./internal/recommend/

bench-als-grid: ## The ALS grid on CATEGORY's valid split (docs/registration/m8-freshness.md) into bench-results/m8/CATEGORY: tools/bundle/als_grid.py on DATA, with the items of the bundle DATA/bundle/VERSION
	@test -n "$(VERSION)" || { echo "set VERSION"; exit 1; }
	cd tools/bundle && uv run --locked python als_grid.py $(abspath $(DATA)) $(abspath $(DATA))/bundle/$(VERSION)/items.ids $(abspath bench-results/m8/$(CATEGORY)) --category $(CATEGORY)

valid-reference: ## The reference on the valid split in Go against bench-results/m8/CATEGORY/valid-ranks.txt.gz, on DATA and the bundle DATA/bundle/VERSION built with the selected settings; RECSERVE_ORT_LIB for its encoder
	RECSERVE_DATA=$(abspath $(DATA)) RECSERVE_BUNDLE=$(abspath $(DATA))/bundle/$(VERSION) RECSERVE_CATEGORY=$(CATEGORY) RECSERVE_VALID_RANKS=$(abspath bench-results/m8/$(CATEGORY)/valid-ranks.txt.gz) \
		$(GO) test -count=1 -timeout 30m -run '^TestRealDataValidReference$$' ./internal/recommend/

bench-windows: ## The popularity reference on CATEGORY's valid split for each window (docs/registration/m8-freshness.md, 6.2) into bench-results/m8/CATEGORY/windows.txt: tools/bundle/fresh.py with the items of the bundle DATA/bundle/VERSION, then the same ranks in Go, which must equal fresh.py's
	@test -n "$(VERSION)" || { echo "set VERSION"; exit 1; }
	mkdir -p bench-results/m8/$(CATEGORY)
	cd tools/bundle && uv run --locked python fresh.py $(abspath $(DATA)) $(abspath $(DATA))/bundle/$(VERSION)/items.ids $(abspath bench-results/m8/$(CATEGORY))/popularity --category $(CATEGORY) > $(abspath bench-results/m8/$(CATEGORY))/windows.txt
	cat bench-results/m8/$(CATEGORY)/windows.txt
	RECSERVE_DATA=$(abspath $(DATA)) RECSERVE_BUNDLE=$(abspath $(DATA))/bundle/$(VERSION) RECSERVE_CATEGORY=$(CATEGORY) RECSERVE_POPULARITY_DIR=$(abspath bench-results/m8/$(CATEGORY))/popularity \
		$(GO) test -count=1 -timeout 30m -run '^TestRealDataPopularityReference$$' ./internal/recommend/

rebuilds: ## Bundles rebuilt every DAYS days through CATEGORY's test period (docs/registration/m8-freshness.md, section 4) into OUT, from DATA, with the ranker of the bundle FROM; a rerun builds only those missing
	@test -n "$(DAYS)" -a -n "$(FROM)" -a -n "$(OUT)" || { echo "set DAYS, FROM and OUT"; exit 1; }
	cd tools/bundle && uv run --locked python rebuilds.py $(abspath $(DATA)) $(abspath $(OUT)) --days $(DAYS) --ranker-from $(abspath $(FROM)) --category $(CATEGORY)

bench-read: ## Test read READ of CATEGORY (docs/registration/m8-freshness.md, section 9) into bench-results/m8/CATEGORY/read-READ.md: TestRealDataFreshness on DATA and B1, DATA/bundle/VERSION; B0, S6 and S8 the bundles' directories, REBUILT the directory of rebuilds' 7, 30 and 30s8; READ1_RANKS for Video_Games' read 2; RANKS, a new directory outside the repository; RECSERVE_ORT_LIB
	@test -n "$(READ)" -a -n "$(VERSION)" -a -n "$(RANKS)" || { echo "set READ, VERSION and RANKS"; exit 1; }
	mkdir -p bench-results/m8/$(CATEGORY)
	RECSERVE_DATA=$(abspath $(DATA)) RECSERVE_BUNDLE=$(abspath $(DATA))/bundle/$(VERSION) RECSERVE_CATEGORY=$(CATEGORY) RECSERVE_READ=$(READ) \
		RECSERVE_BUNDLE_B0=$(if $(B0),$(abspath $(B0))) RECSERVE_BUNDLE_S6=$(if $(S6),$(abspath $(S6))) RECSERVE_BUNDLE_S8=$(if $(S8),$(abspath $(S8))) \
		RECSERVE_REBUILT_7=$(if $(REBUILT),$(abspath $(REBUILT))/7) RECSERVE_REBUILT_30=$(if $(REBUILT),$(abspath $(REBUILT))/30) \
		RECSERVE_REBUILT_30S8=$(if $(REBUILT),$(abspath $(REBUILT))/30s8) RECSERVE_READ1_RANKS=$(if $(READ1_RANKS),$(abspath $(READ1_RANKS))) \
		RECSERVE_FRESHNESS_OUT=$(abspath bench-results/m8/$(CATEGORY))/read-$(READ).md RECSERVE_RANKS_DIR=$(abspath $(RANKS)) \
		$(GO) test -count=1 -timeout 0 -run '^TestRealDataFreshness$$' ./internal/recommend/

bench-stream: images ## The stream state's cost in serving (docs/BENCHMARKS.md, Stream state in serving): STEP gate, micro, phase1, swap, profile or phase2, in that order, on DATA's bundles; RECSERVE_ORT_LIB for gate and micro
	$(GO) build -buildvcs=false -o bin/ ./cmd/...
	DATA=$(abspath $(DATA)) scripts/bench-stream.sh $(STEP)

bench-exact: ## Exact search of every item of the bundle DATA/bundle/VERSION on CPU 1 into bench-results/m8/exact-search.txt (README, Scale)
	mkdir -p bench-results/m8
	RECSERVE_BUNDLE=$(abspath $(DATA))/bundle/$(VERSION) taskset -c 1 $(GO) test -count=1 -run '^$$' -bench '^BenchmarkRealExactSearch$$' \
		-benchtime 2000x -count 10 -cpu 1 ./internal/recommend/ | tee bench-results/m8/exact-search.txt

report: ## Charts of docs/BENCHMARKS.md's M6 results into bench-results/m6/figures (needs uv)
	cd tools/report && uv run --locked python report.py $(abspath bench-results/m6) $(abspath bench-results/m6/figures)

bench-profile: build ## CPU profiles of the gateway and a replica under load in compose (docs/BENCHMARKS.md); DATA, VERSION
	$(BENCH) scripts/bench-profile.sh

bench-compare: build ## Before and after an optimization: BEFORE, a commit; vec's benchmark MICRO (Dot), annbench on the glove graph ANN_GRAPH in ANN_DATA if set, the service at RATES (200 400); DATA, VERSION, DOCKER_BUILD_FLAGS, OUT
	$(BENCH) MICRO=$(MICRO) ANN_GRAPH=$(ANN_GRAPH) ANN_DATA=$(ANN_DATA) OUT=$(OUT) DOCKER_BUILD_FLAGS="$(DOCKER_BUILD_FLAGS)" scripts/bench-compare.sh $(BEFORE)
