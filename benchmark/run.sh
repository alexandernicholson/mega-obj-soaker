#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# Benchmark: mega-obj-soaker vs super-obj-soaker vs AWS S3 CLI
# ──────────────────────────────────────────────────────────────
#
# Requires:
#   - docker compose (v2)
#   - aws cli
#   - python3 + boto3
#   - mega-obj-soaker binary (cargo build --release)
#   - super-obj-soaker checkout as sibling directory
#
# Usage:
#   ./benchmark/run.sh                    # defaults: 100 files, 10MB each
#   ./benchmark/run.sh --files 200 --size 50  # 200 files, 50MB each
#   ./benchmark/run.sh --runs 5           # 5 runs per tool

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SUPER_DIR="$(cd "$PROJECT_DIR/../super-obj-soaker" 2>/dev/null && pwd || echo "")"

# ── Defaults ──────────────────────────────────────────────────
NUM_FILES=100
FILE_SIZE_MB=10
NUM_RUNS=3
MAX_PROCS=16
OPT_INTERVAL=2
ENDPOINT="http://localhost:8333"
BUCKET="benchmark"
PREFIX="data"

# ── Parse arguments ───────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --files)      NUM_FILES="$2"; shift 2 ;;
        --size)       FILE_SIZE_MB="$2"; shift 2 ;;
        --runs)       NUM_RUNS="$2"; shift 2 ;;
        --max-procs)  MAX_PROCS="$2"; shift 2 ;;
        --endpoint)   ENDPOINT="$2"; shift 2 ;;
        --help|-h)
            echo "Usage: $0 [--files N] [--size MB] [--runs N] [--max-procs N] [--endpoint URL]"
            echo ""
            echo "Options:"
            echo "  --files N       Number of test files to generate (default: 100)"
            echo "  --size MB       Size of each file in MB (default: 10)"
            echo "  --runs N        Number of benchmark runs per tool (default: 3)"
            echo "  --max-procs N   MAX_PROCESSES for soaker tools (default: 16)"
            echo "  --endpoint URL  S3 endpoint URL (default: http://localhost:8333)"
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

TOTAL_MB=$((NUM_FILES * FILE_SIZE_MB))
UPLOAD_DIR=$(mktemp -d)
RESULTS_FILE=$(mktemp)

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# ── Helpers ───────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "Cleaning up..."
    rm -rf "$UPLOAD_DIR"
    rm -rf /tmp/bench-dl-*
    cd "$PROJECT_DIR" && docker compose down -v 2>/dev/null || true
}
trap cleanup EXIT

drop_caches() {
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null 2>&1 || true
}

# Returns elapsed time in milliseconds
time_ms() {
    local start end
    start=$(date +%s%N)
    "$@" > /dev/null 2>&1
    end=$(date +%s%N)
    echo $(( (end - start) / 1000000 ))
}

run_benchmark() {
    local name="$1"
    shift
    local cmd=("$@")
    local best_ms=999999999
    local total_ms=0

    echo "─── $name ───"

    for run in $(seq 1 "$NUM_RUNS"); do
        rm -rf /tmp/bench-dl-current
        mkdir -p /tmp/bench-dl-current
        drop_caches

        local start end elapsed_ms
        start=$(date +%s%N)
        "${cmd[@]}"
        end=$(date +%s%N)
        elapsed_ms=$(( (end - start) / 1000000 ))

        local elapsed_s speed
        elapsed_s=$(echo "scale=2; $elapsed_ms / 1000" | bc)
        speed=$(echo "scale=1; $TOTAL_MB * 1000 / $elapsed_ms" | bc)

        local count
        count=$(find /tmp/bench-dl-current -type f | wc -l)

        echo "  Run $run: ${elapsed_s}s  ${speed} MB/s  (${count} files)"

        total_ms=$((total_ms + elapsed_ms))
        if [ "$elapsed_ms" -lt "$best_ms" ]; then
            best_ms=$elapsed_ms
        fi
    done

    local avg_ms best_s avg_s best_speed avg_speed
    avg_ms=$((total_ms / NUM_RUNS))
    best_s=$(echo "scale=2; $best_ms / 1000" | bc)
    avg_s=$(echo "scale=2; $avg_ms / 1000" | bc)
    best_speed=$(echo "scale=1; $TOTAL_MB * 1000 / $best_ms" | bc)
    avg_speed=$(echo "scale=1; $TOTAL_MB * 1000 / $avg_ms" | bc)

    echo "  Best: ${best_s}s (${best_speed} MB/s)  Avg: ${avg_s}s (${avg_speed} MB/s)"
    echo "$name|$best_s|$best_speed|$avg_s|$avg_speed" >> "$RESULTS_FILE"
}

# ── Setup ─────────────────────────────────────────────────────
echo "=== Benchmark Configuration ==="
echo "  Files:      $NUM_FILES x ${FILE_SIZE_MB}MB = ${TOTAL_MB}MB"
echo "  Runs:       $NUM_RUNS per tool"
echo "  Max procs:  $MAX_PROCS"
echo "  Endpoint:   $ENDPOINT"
echo ""

echo "Starting SeaweedFS..."
cd "$PROJECT_DIR" && docker compose up -d seaweedfs 2>&1 | tail -1

echo "Waiting for SeaweedFS..."
for i in $(seq 1 60); do
    if curl -s -o /dev/null -w "%{http_code}" "$ENDPOINT" 2>/dev/null | grep -q 200; then
        echo "SeaweedFS ready after ${i}s"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "SeaweedFS failed to start"
        exit 1
    fi
    sleep 1
done

echo "Generating test files..."
for i in $(seq -w 1 "$NUM_FILES"); do
    dd if=/dev/urandom of="$UPLOAD_DIR/file-${i}.bin" bs=1M count="$FILE_SIZE_MB" 2>/dev/null
done
echo "Generated $NUM_FILES files (${TOTAL_MB}MB)"

echo "Creating bucket and uploading..."
aws --endpoint-url "$ENDPOINT" s3 mb "s3://$BUCKET" 2>/dev/null || true
for i in $(seq -w 1 "$NUM_FILES"); do
    aws --endpoint-url "$ENDPOINT" s3 cp "$UPLOAD_DIR/file-${i}.bin" "s3://$BUCKET/$PREFIX/file-${i}.bin" --quiet 2>/dev/null &
    if (( $(echo "$i" | sed 's/^0*//') % 10 == 0 )); then
        wait
    fi
done
wait

UPLOADED=$(aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET/$PREFIX/" 2>/dev/null | wc -l)
echo "Uploaded $UPLOADED files to s3://$BUCKET/$PREFIX/"
echo ""

# ── Run benchmarks ────────────────────────────────────────────
echo "=== Running Benchmarks ==="
echo ""

# 1. AWS S3 CLI
run_benchmark "AWS S3 CLI" \
    aws --endpoint-url "$ENDPOINT" s3 sync "s3://$BUCKET/$PREFIX/" /tmp/bench-dl-current/ --quiet

echo ""

# 2. super-obj-soaker (Python)
if [ -n "$SUPER_DIR" ] && [ -f "$SUPER_DIR/s3_optimized_downloader.py" ]; then
    run_benchmark "super-obj-soaker (Python)" \
        python3 "$SUPER_DIR/s3_optimized_downloader.py" "s3://$BUCKET/$PREFIX" /tmp/bench-dl-current \
        --endpoint-url "$ENDPOINT" --region us-east-1 --log-level ERROR
else
    echo "─── super-obj-soaker (Python) ───"
    echo "  SKIPPED: $PROJECT_DIR/../super-obj-soaker not found"
fi

echo ""

# 3. mega-obj-soaker (Rust)
MEGA_BIN="$PROJECT_DIR/target/release/mega-obj-soaker"
if [ -x "$MEGA_BIN" ]; then
    run_benchmark "mega-obj-soaker (Rust)" \
        "$MEGA_BIN" "s3://$BUCKET/$PREFIX" /tmp/bench-dl-current \
        --endpoint-url "$ENDPOINT" --region us-east-1 --log-level ERROR
else
    echo "─── mega-obj-soaker (Rust) ───"
    echo "  SKIPPED: build with 'cargo build --release' first"
fi

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "=== Summary ==="
echo ""
printf "%-28s %8s %12s %8s %12s\n" "Tool" "Best" "Best MB/s" "Avg" "Avg MB/s"
printf "%-28s %8s %12s %8s %12s\n" "---" "---" "---" "---" "---"
while IFS='|' read -r name best_s best_speed avg_s avg_speed; do
    printf "%-28s %7ss %10s %7ss %10s\n" "$name" "$best_s" "$best_speed" "$avg_s" "$avg_speed"
done < "$RESULTS_FILE"

rm -f "$RESULTS_FILE"
echo ""
echo "Dataset: ${NUM_FILES} files x ${FILE_SIZE_MB}MB = ${TOTAL_MB}MB | Runs: ${NUM_RUNS} | MAX_PROCESSES: ${MAX_PROCS}"
