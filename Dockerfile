# Images of the gateway, the annshard replica and the ingest consumer:
#   docker build --target gateway -t recserve/gateway .
# All three run as the distroless nonroot user (65532). The gateway image holds
# ONNX Runtime at /usr/local/lib/libonnxruntime.so.1.31.1, for -ort-lib.

FROM golang:1.27.4-trixie@sha256:b54b2639de8c1bfe538ec5ffc45f29b1932235ac5f6d63c059853646677112d9 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd cmd
COPY internal internal
COPY proto proto
# The gateway and annshard link ONNX Runtime's Go binding, which needs cgo;
# ingest does not, and is built static.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags='-s -w' -o /out/ ./cmd/gateway ./cmd/annshard && \
    CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/ ./cmd/ingest

FROM golang:1.27.4-trixie@sha256:b54b2639de8c1bfe538ec5ffc45f29b1932235ac5f6d63c059853646677112d9 AS ort
ADD --checksum=sha256:74a6a52994616c4d18b33db1e13c226333a70a6ba07480a5cab9c2a50dff1430 \
    https://files.pythonhosted.org/packages/fc/0a/c887bcb84a9455b2723825381a77af38611a80f996f804e20fde5f671b63/onnxruntime-1.31.1-cp313-cp313-manylinux_2_28_x86_64.whl /ort.whl
RUN apt-get update && apt-get install -y --no-install-recommends unzip && \
    unzip -q -j -d /usr/local/lib /ort.whl onnxruntime/capi/libonnxruntime.so.1.31.1

# glibc and libstdc++, which ONNX Runtime needs.
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:cc66d222f0a7ec996b78a99fe76a969bc80b539a5016ad866c14d1949bc6d0ab AS gateway
COPY --from=ort /usr/local/lib/libonnxruntime.so.1.31.1 /usr/local/lib/
COPY --from=build /out/gateway /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/gateway"]

# glibc only: annshard links the binding but never loads ONNX Runtime.
FROM gcr.io/distroless/base-debian13:nonroot@sha256:7fe4df6a4413adf5889960d345a28b4574aea0872ea47684c7244c8d88027dca AS annshard
COPY --from=build /out/annshard /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/annshard"]

FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329 AS ingest
COPY --from=build /out/ingest /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/ingest"]

# The faulty replica of the fault runs (docs/BENCHMARKS.md, F6), built only
# when this target is named; make images does not.
FROM build AS faultshard-build
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags='-s -w' -o /out/ ./cmd/faultshard

FROM gcr.io/distroless/base-debian13:nonroot@sha256:7fe4df6a4413adf5889960d345a28b4574aea0872ea47684c7244c8d88027dca AS faultshard
COPY --from=faultshard-build /out/faultshard /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/faultshard"]
