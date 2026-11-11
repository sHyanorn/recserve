module github.com/sHyanorn/recserve

go 1.27.3

require (
	github.com/prometheus/client_golang v1.25.0
	github.com/sony/gobreaker/v2 v2.4.0
	github.com/twmb/franz-go v1.22.3
	github.com/twmb/franz-go/pkg/kfake v0.0.0-20261109062748-d6ea02a174bc
	github.com/twmb/franz-go/pkg/kmsg v1.15.1
	github.com/valkey-io/valkey-go v1.0.79
	github.com/yalue/onnxruntime_go v1.38.0
	golang.org/x/sync v0.25.0
	golang.org/x/sys v0.48.0
	golang.org/x/time v0.16.0
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pierrec/lz4/v4 v4.1.33 // indirect
	github.com/prometheus/client_model v0.6.3 // indirect
	github.com/prometheus/common v0.72.0 // indirect
	github.com/prometheus/procfs v0.22.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260817212433-ac3dfec99bb1 // indirect
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.6.2 // indirect
)

tool (
	google.golang.org/grpc/cmd/protoc-gen-go-grpc
	google.golang.org/protobuf/cmd/protoc-gen-go
)
