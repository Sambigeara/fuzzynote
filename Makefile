.PHONY: test

FILE     = VERSION
VERSION := $(shell git describe --abbrev=0 --tags 2>/dev/null)
DATE    := $(shell git log -1 --format=%ct $(VERSION) 2>/dev/null)

# Cover non-git case (compiling from release source package)
# The date will be set to the time of compilation, the tag will be consistent
ifeq ($(VERSION),)
	VERSION := $(shell cat $(FILE))
	DATE    := $(shell date +%s)
endif

build:
	@go build \
	-buildmode=pie \
	-ldflags="-X 'main.version=$(VERSION)' -X 'main.date=$(DATE)'" \
	-o bin/fzn ./cmd/term
	@echo "Build complete"

# The following are sequential commands, but I'm separating to reduce the chance of mistakes...
new-tag:
	test $(tag)
	echo "$(tag)" > $(FILE)
	git add $(FILE)
	git commit -m "Release: $(tag)"
	git tag $(tag)
release:
	git push
	git push --tags
	goreleaser release --rm-dist

test:
	go test ./... -count=1

debug:
	dlv debug ./cmd/term/main.go

test-debug:
	dlv test pkg/service/*
