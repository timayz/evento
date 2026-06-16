.PHONY: gen test tidy

# Generate the gRPC bindings into ./eventov1 from ../../proto.
# Requires `buf` (https://buf.build). The remote plugins need network access.
gen:
	buf generate

# Run the SDK tests. Builds the server first so the tests can launch it.
# Alternatively set EVENTO_GRPC_ADDR to point at an already-running server.
test:
	cargo build -p evento-server --manifest-path ../../Cargo.toml
	go test ./...

tidy:
	go mod tidy
