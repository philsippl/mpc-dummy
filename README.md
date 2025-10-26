# 🦆 vector-mpc-poc

A proof of concept for vector similarity search (via inner product) using the MPC primitives developed in [iris-mpc](https://github.com/worldcoin/iris-mpc).

## Running

### Using the Shell Script (Local Testing)

Run all 3 parties on localhost:

```bash
./run_remote_local.sh
```

This starts 3 actors on ports 7001, 7002, 7003.

### Manual Execution

Run each party separately:

```bash
# Terminal 1 - Party 0
PARTY_INDEX=0 cargo run --release

# Terminal 2 - Party 1
PARTY_INDEX=1 cargo run --release

# Terminal 3 - Party 2
PARTY_INDEX=2 cargo run --release
```

### Configuration

Configure via environment variables or CLI args:

```bash
# Database size
MAX_DB=1000000

# Network parallelism
CONNECTION_PARALLELISM=4
SESSION_PER_REQUEST=8

# Processing parallelism
REQUEST_PARALLELISM=1

# Addresses (comma-separated)
ADDRESSES=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
```

## Benchmarks

Achieves around 10 req/s @ 10M database on c8g.8xlarge (32 vCPUs, 64 GiB RAM, 15 Gibps Networking):

```
INFO vector_mpc_poc: ═══════════════════════════════════════════════════════
INFO vector_mpc_poc: Actor 0 FINAL SUMMARY (100 requests)
INFO vector_mpc_poc: Total:       min=94.464ms max=147.071ms mean=99.58ms
INFO vector_mpc_poc: ═══════════════════════════════════════════════════════
```
