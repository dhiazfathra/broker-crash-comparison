GOOS_BIN := bin/consumer

.PHONY: build bench test lint clean

build:
	go build -o bin/producer ./cmd/producer
	go build -o bin/analyze ./cmd/analyze
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -o $(GOOS_BIN) ./cmd/consumer

bench: build
	./scripts/run.sh

saturation: build
	./scripts/saturation.sh

test:
	go test ./...

lint:
	golangci-lint run

clean:
	docker compose down -v
	rm -rf bin results/current
