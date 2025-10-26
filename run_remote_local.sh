#!/bin/bash

# Script to run 3 MPC actors in remote mode locally
# Kills all instances when the script is terminated

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Array to store PIDs
PIDS=()

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Shutting down all actors...${NC}"
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "${RED}Killing actor with PID $pid${NC}"
            kill "$pid" 2>/dev/null || true
        fi
    done
    wait
    echo -e "${GREEN}All actors stopped${NC}"
    exit 0
}

# Set up trap to catch signals and run cleanup
trap cleanup SIGINT SIGTERM EXIT

echo -e "${GREEN}Starting 3 MPC actors in remote mode...${NC}"

# Start actor 0
echo -e "${BLUE}Starting Actor 0 (port 7001)...${NC}"
PARTY_INDEX=0 cargo run --release &
PIDS+=($!)
sleep 1

# Start actor 1
echo -e "${BLUE}Starting Actor 1 (port 7002)...${NC}"
PARTY_INDEX=1 cargo run --release &
PIDS+=($!)
sleep 1

# Start actor 2
echo -e "${BLUE}Starting Actor 2 (port 7003)...${NC}"
PARTY_INDEX=2 cargo run --release &
PIDS+=($!)
sleep 1

echo -e "\n${GREEN}All actors started!${NC}"
echo -e "${YELLOW}Press Ctrl+C to stop all actors${NC}\n"

# Wait for all background processes
wait
