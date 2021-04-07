build:
	go build -o bin/fzn ./cmd/term

build-wasm:
	env GOARCH=wasm GOOS=js go build -o web/app.wasm ./cmd/web/main.go
	go build -o bin/fzn-server ./cmd/web/

run:
	./bin/fzn-server

test:
	go test ./... -count=1

debug:
	dlv debug ./cmd/term/main.go
	#dlv debug ./cmd/term/main.go -- --root=test

test-debug:
	dlv test pkg/service/*
	#dlv test pkg/service/* -- -test.run TestServiceMove
