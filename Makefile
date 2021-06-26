.PHONY: test

FILE     = VERSION
TAG     := $(shell git describe --abbrev=0 --tags 2>/dev/null)
DATE    := $(shell git log -1 --format=%ct $(TAG) 2>/dev/null)
VERSION := $(if $(TAG),$(TAG) $(DATE),$(cat $(FILE)))

flush-version:
	@echo "$(VERSION)" > $(FILE)

build:
	make flush-version
	@go build \
	-buildmode=pie \
	-ldflags="-X 'main.version=$(TAG)' -X 'main.date=$(DATE)'" \
	-o bin/fzn ./cmd/term
	@echo "Build complete"

release:
	make flush-version
	goreleaser release --rm-dist

test:
	go test ./... -count=1

debug:
	dlv debug ./cmd/term/main.go

test-debug:
	dlv test pkg/service/*
