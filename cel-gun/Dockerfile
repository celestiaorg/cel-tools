# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY ../go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY .. .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o main .

# Final stage
FROM alpine:latest

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/main .
COPY --from=builder /app/load.yaml .
COPY --from=builder /app/ammo.txt .

# Set environment variables (these should be overridden at runtime)
ENV GUN_NETWORK="arabica-11"
ENV GUN_TARGET="/dnsaddr/da-bootstrapper-2.celestia-arabica-11.com/p2p/12D3KooWCMGM5eZWVfCN9ZLAViGfLUWAfXP5pCm78NFKb9jpBtua"
ENV GUN_NAMESPACE="00000000000000000000000000000000000000000000004365726f41"
ENV GUN_HEIGHT="6490556"

# Run the application
CMD ["./main"]
