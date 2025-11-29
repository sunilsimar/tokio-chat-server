# Build stage
FROM rust:1.90.0 as builder

WORKDIR /app

# Copy manifest files
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build the release binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install required libraries for Rust binary
RUN apt-get update && \
    apt-get install -y libc6 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/chat-compressor /app/chat-compressor

# Expose the port
EXPOSE 8080

# Run the binary
CMD ["./chat-compressor"]
