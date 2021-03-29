build:
	go build -o bin/fzn ./cmd/fuzzy-note

test:
	go test ./... -count=1

debug:
	dlv debug ./cmd/fuzzy-note/main.go
	#dlv debug ./cmd/fuzzy-note/main.go -- --root=test

test-debug:
	dlv test pkg/service/*
	#dlv test pkg/service/* -- -test.run TestServiceMove
