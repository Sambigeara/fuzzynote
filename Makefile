build:
	go build -o bin/fzn ./cmd/term

test:
	go test ./... -count=1

debug:
	dlv debug ./cmd/term/main.go
	#dlv debug ./cmd/term/main.go -- --root=test

test-debug:
	dlv test pkg/service/*
	#dlv test pkg/service/* -- -test.run TestServiceMove
