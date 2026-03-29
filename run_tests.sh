#!/bin/bash
set -e

# Function to tear down Docker containers
tear_down() {
    echo "Tearing down Docker containers..."
    docker-compose down -v 2>/dev/null || true
}

# Tear down existing containers before starting
tear_down

# Start SeaweedFS
echo "Starting SeaweedFS..."
docker-compose up -d seaweedfs

# Wait for SeaweedFS to be ready
check_seaweedfs() {
    local max_attempts=60
    local attempt=1

    echo "Waiting for SeaweedFS to be ready..."
    while true; do
        if curl -s -o /dev/null -w "%{http_code}" http://localhost:8333 2>/dev/null | grep -q 200; then
            echo "SeaweedFS is ready!"
            return 0
        fi

        if [ $attempt -eq $max_attempts ]; then
            echo "SeaweedFS is not ready after $max_attempts attempts. Exiting."
            return 1
        fi

        printf '.'
        sleep 2
        ((attempt++))
    done
}

if ! check_seaweedfs; then
    tear_down
    exit 1
fi

# Run integration tests locally (requires cargo)
echo "Running tests..."
MIN_PROCESSES=2 \
MAX_PROCESSES=10 \
MAX_SPEED=200 \
OPTIMIZATION_INTERVAL=1 \
MAX_RETRIES=3 \
RETRY_DELAY=1 \
AWS_ACCESS_KEY_ID=test \
AWS_SECRET_ACCESS_KEY=test \
cargo test --release -- --nocapture

TEST_EXIT_CODE=$?

tear_down

echo "Tests completed with exit code: $TEST_EXIT_CODE"
exit $TEST_EXIT_CODE
