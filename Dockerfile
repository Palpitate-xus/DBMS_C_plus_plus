# ============================================================
# DBMS_C_plus_plus — Multi-stage Docker build
# Stage 1: compile the C++17 source with g++ and OpenSSL
# Stage 2: slim runtime image with just the binary + libssl
# ============================================================

# ---- Builder stage ----
FROM ubuntu:24.04 AS builder

# Avoid interactive prompts during apt install
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    make \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy source tree
COPY src/ src/
COPY scripts/build.sh CMakeLists.txt ./

# Build the binary (build.sh detects OpenSSL automatically)
RUN chmod +x scripts/build.sh && scripts/build.sh

# ---- Runtime stage ----
FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

# Runtime dependencies only
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary from builder
COPY --from=builder /app/dbms_main /app/dbms_main

# Directory that will hold database files (mounted as a volume)
RUN mkdir -p /data
VOLUME ["/data"]

# Default TCP port for server mode
EXPOSE 9999

# Default: run in interactive mode
# Override with: docker run ... ./dbms_main --server 9999
CMD ["./dbms_main"]
