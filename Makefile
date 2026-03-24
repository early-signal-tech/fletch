VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

LDFLAGS := -s -w -X main.Version=$(VERSION)

.PHONY: build build-all clean

build:
	go build -ldflags="$(LDFLAGS)" -o dist/fletch .

build-all: dist/fletch-darwin-arm64 dist/fletch-darwin-amd64

dist/fletch-darwin-arm64:
	CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 go build -ldflags="$(LDFLAGS)" -o $@ .

dist/fletch-darwin-amd64:
	CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 go build -ldflags="$(LDFLAGS)" -o $@ .

clean:
	rm -rf dist/
