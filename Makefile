.PHONY: run-server
run-server:
	go run ./cmd/server/server.go

.PHONY: run-client
run-client:
	go run ./cmd/client/client.go

.PHONY: test
test:
	go test -v ./...

.PHONY: bench
bench:
	go test -v -bench=. ./...