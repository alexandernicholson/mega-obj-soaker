# Benchmark

Compares download performance of three S3 download tools against a local [SeaweedFS](https://github.com/seaweedfs/seaweedfs) instance:

| Tool | Description |
|------|-------------|
| **AWS S3 CLI** | `aws s3 sync` — the standard baseline |
| **super-obj-soaker** | Python multi-process self-optimising downloader |
| **mega-obj-soaker** | Rust actor-based self-optimising downloader (this project) |

## Quick Start

```bash
# From the project root
./benchmark/run.sh
```

## Requirements

- Docker Compose v2 (for SeaweedFS)
- AWS CLI (`pip install awscli`)
- Python 3 + boto3 (`pip install boto3`)
- [super-obj-soaker](https://github.com/alexandernicholson/super-obj-soaker) cloned as a sibling directory
- mega-obj-soaker built: `cargo build --release`

## Options

```
./benchmark/run.sh [--files N] [--size MB] [--runs N] [--max-procs N] [--endpoint URL]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--files N` | `100` | Number of test files to generate |
| `--size MB` | `10` | Size of each file in MB |
| `--runs N` | `3` | Benchmark runs per tool |
| `--max-procs N` | `16` | `MAX_PROCESSES` for soaker tools |
| `--endpoint URL` | `http://localhost:8333` | S3 endpoint URL |

### Examples

```bash
# Default: 100 x 10MB = 1GB, 3 runs
./benchmark/run.sh

# Larger dataset: 200 x 50MB = 10GB, 5 runs
./benchmark/run.sh --files 200 --size 50 --runs 5

# Stress test with high concurrency
./benchmark/run.sh --files 500 --size 1 --max-procs 64
```

## What It Does

1. Starts a local SeaweedFS container via Docker Compose
2. Generates random binary files of the specified count and size
3. Uploads all files to SeaweedFS
4. Runs each tool N times, dropping filesystem caches between runs
5. Reports best and average times with throughput in MB/s
6. Tears down SeaweedFS and cleans up temp files

## Methodology

- **Cache clearing**: `sync && echo 3 > /proc/sys/vm/drop_caches` between runs (requires sudo, silently skipped if unavailable)
- **Fresh destination**: Download directory is deleted and recreated before each run
- **Identical configuration**: All tools use the same `MAX_PROCESSES=16`, `OPTIMIZATION_INTERVAL=2s`, and connect to the same SeaweedFS instance
- **Wall-clock timing**: Measured with nanosecond precision via `date +%s%N`
- **File verification**: Each run confirms the correct number of files were downloaded
