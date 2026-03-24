# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build (no CGO needed - using pgx which is pure Go)
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /tracker ./cmd/tracker

# Runtime stage
FROM alpine:3.20

# Install runtime dependencies
RUN apk add --no-cache ca-certificates tzdata

# Create non-root user
RUN adduser -D -u 1000 tracker

WORKDIR /app

# Copy binary and static files
COPY --from=builder /tracker /app/tracker
COPY static /app/static
COPY config.toml /app/config.toml

# Change ownership
RUN chown -R tracker:tracker /app

USER tracker

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run
ENTRYPOINT ["/app/tracker"]
CMD ["-config", "/app/config.toml"]
