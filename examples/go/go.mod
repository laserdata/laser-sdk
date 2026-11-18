module github.com/laserdata/laser-sdk/examples/go

go 1.25.0

require github.com/laserdata/laser-sdk/foreign/go v0.0.0

require (
	github.com/apache/iggy/foreign/go v0.10.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.20.3 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)

replace github.com/laserdata/laser-sdk/foreign/go => ../../foreign/go
