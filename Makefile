BINARY  := zeaos
CMD     := ./cmd/zeaos
TAGS    := duckdb_arrow
LDFLAGS :=

.PHONY: build install test clean

build:
	go build -tags $(TAGS) $(LDFLAGS) -o $(BINARY) $(CMD)

install:
	go install -tags $(TAGS) $(LDFLAGS) $(CMD)

test:
	go test -tags $(TAGS) ./...

clean:
	rm -f $(BINARY)
