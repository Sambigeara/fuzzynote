VERSION := $(shell git describe --abbrev=0 --tags)

build:
	go build -ldflags="-X main.version=$(VERSION)" -o bin/fzn ./cmd/term

test:
	go test ./... -count=1

debug:
	dlv debug ./cmd/term/main.go
	#dlv debug ./cmd/term/main.go -- --root=test

test-debug:
	dlv test pkg/service/*
	#dlv test pkg/service/* -- -test.run TestServiceMove
