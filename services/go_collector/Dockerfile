# ==============================================================================
# Build Stage
# ==============================================================================
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Cache dependencies
COPY go.mod ./
RUN go mod download || true

# Copy source code
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /bin/go-collector .

# ==============================================================================
# Production Runtime Stage
# ==============================================================================
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata && \
    addgroup -S aether && adduser -S aether -G aether

WORKDIR /app
COPY --from=builder /bin/go-collector /app/go-collector

USER aether

EXPOSE 9102

ENTRYPOINT ["/app/go-collector"]
