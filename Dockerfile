# Images of the gateway, the annshard replica and the ingest consumer:
#   docker build --target gateway -t recserve/gateway .
# All three run as the distroless nonroot user (65532). The gateway image holds
# ONNX Runtime at /usr/local/lib/libonnxruntime.so.1.31.1, for -ort-lib.

FROM golang:1.27.3-trixie@sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80 AS build
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

FROM golang:1.27.3-trixie@sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80 AS ort
ADD --checksum=sha256:c849e36a1ef3e22d4ff217b9d1ce4a3cf5b9467b975610913ae959ad5b16bdc9 \
    https://github.com/microsoft/onnxruntime/releases/download/v1.31.1/onnxruntime-linux-x64-1.31.1.tgz /ort.tgz
RUN tar -xzf /ort.tgz -C /usr/local/lib --strip-components=2 onnxruntime-linux-x64-1.31.1/lib/libonnxruntime.so.1.31.1

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
