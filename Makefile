BINARY  := zeaos
CMD     := ./cmd/zeaos
TAGS    := duckdb_arrow
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

.PHONY: build install test clean docker-build docker-test docker-down

build:
	go build -tags $(TAGS) $(LDFLAGS) -o $(BINARY) $(CMD)

install:
	go install -tags $(TAGS) $(LDFLAGS) $(CMD)

test:
	go test -tags $(TAGS) ./... ./zeaberg/...

clean:
	rm -f $(BINARY)

# Build the runtime Docker image
docker-build:
	docker build --target runtime --build-arg VERSION=$(VERSION) -t zeaos:$(VERSION) .

# Run the full test suite inside Docker (with MinIO for S3 tests)
docker-test:
	docker compose run --rm test

# Tear down test infrastructure
docker-down:
	docker compose down -v
