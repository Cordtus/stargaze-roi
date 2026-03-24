# Stargaze ROI Tracker Makefile

.PHONY: build run server dev clean test docker-build docker-run deploy

# Build the binary
build:
	go build -o bin/tracker ./cmd/tracker

# Run the tracker (processor + server)
run: build
	./bin/tracker -config config.toml

# Run in server-only mode (no event processing)
server: build
	./bin/tracker -config config.toml -server-only

# Development mode with auto-reload (requires air)
dev:
	air -c .air.toml || (go install github.com/air-verse/air@latest && air -c .air.toml)

# Clean build artifacts
clean:
	rm -rf bin/

# Run tests
test:
	go test -v ./...

# Format code
fmt:
	go fmt ./...

# Lint code (requires golangci-lint)
lint:
	golangci-lint run

# Download dependencies
deps:
	go mod download
	go mod tidy

# Docker build
docker-build:
	docker build -t starmos-roi-tracker:latest .

# Docker run (requires DATABASE_URL env var)
docker-run: docker-build
	docker run -d \
		--name starmos-tracker \
		-p 8080:8080 \
		-e DATABASE_URL="$(DATABASE_URL)" \
		starmos-roi-tracker:latest

# Docker stop and remove
docker-stop:
	docker stop starmos-tracker || true
	docker rm starmos-tracker || true

# Deploy to Fly.io
deploy:
	fly deploy

# Set database URL secret on Fly.io
fly-secret:
	@echo "Run: fly secrets set DATABASE_URL='postgres://user:pass@host:5432/dbname'"

# View Fly.io logs
fly-logs:
	fly logs

# SSH into Fly.io instance
fly-ssh:
	fly ssh console

# View current stats from running instance
stats:
	curl -s http://localhost:8080/api/stats | jq .

# View recent transactions
txs:
	curl -s 'http://localhost:8080/api/transactions?limit=10' | jq .

# Health check
health:
	curl -s http://localhost:8080/health | jq .
