.PHONY: proto build test test-race test-integration test-acceptance lint docker-build docker-up docker-test clean

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOVET=$(GOCMD) vet
GOMOD=$(GOCMD) mod
BINARY_DIR=bin

# Proto parameters
PROTOC=protoc
PROTO_DIR=proto
PROTO_OUT=proto

# All binaries
BINARIES=namenode datanode resourcemanager nodemanager hdfs mapreduce mapworker

# Build all binaries
build: $(addprefix $(BINARY_DIR)/,$(BINARIES))

$(BINARY_DIR)/%: cmd/%/main.go
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) -o $@ ./cmd/$*

# Generate protobuf code
proto:
	$(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/*.proto

# Run all unit tests
test:
	$(GOTEST) ./pkg/... -v -count=1

# Run tests with race detector
test-race:
	$(GOTEST) ./pkg/... -v -race -count=1

# Run integration tests
test-integration:
	$(GOTEST) ./test/integration/... -v -count=1 -timeout 5m

# Run acceptance tests (requires Docker Compose cluster)
test-acceptance:
	$(GOTEST) ./test/acceptance/... -v -count=1 -timeout 30m

# Run go vet
lint:
	$(GOVET) ./...

# Build Docker image
docker-build:
	docker build -t mini-hadoop -f docker/Dockerfile .

# Start Docker Compose cluster
docker-up:
	docker compose -f docker/docker-compose.yml up -d

# Run acceptance tests in Docker
docker-test: docker-build docker-up
	docker compose -f docker/docker-compose.yml exec client go test ./test/acceptance/... -v -timeout 30m

# Stop Docker Compose cluster
docker-down:
	docker compose -f docker/docker-compose.yml down -v

# Clean build artifacts
clean:
	rm -rf $(BINARY_DIR)
	$(GOMOD) tidy

# Tidy dependencies
tidy:
	$(GOMOD) tidy
