# Builds the R1 command with cgo and runs it on distroless/cc (D012).
# Build from experiments/m0: docker build -f onnx/Dockerfile -t recserve-m0-onnx .
# Run: docker run --rm -v DIR:/models:ro recserve-m0-onnx -mode parity
FROM golang:1.27.3-trixie@sha256:af1bfc2614ebb1dbd31af514b78740fd678850e76498762b425ecee4c5c6ea80 AS build
ADD --checksum=sha256:c849e36a1ef3e22d4ff217b9d1ce4a3cf5b9467b975610913ae959ad5b16bdc9 \
    https://github.com/microsoft/onnxruntime/releases/download/v1.31.1/onnxruntime-linux-x64-1.31.1.tgz /tmp/ort.tgz
RUN mkdir /ort && tar xzf /tmp/ort.tgz -C /ort --strip-components=1
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -trimpath -o /out/r1 ./onnx

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:cc66d222f0a7ec996b78a99fe76a969bc80b539a5016ad866c14d1949bc6d0ab
COPY --from=build /ort/lib/libonnxruntime.so.1.31.1 /usr/local/lib/libonnxruntime.so
COPY --from=build /out/r1 /r1
ENTRYPOINT ["/r1", "-lib", "/usr/local/lib/libonnxruntime.so", "-dir", "/models"]
