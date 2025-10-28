#!/bin/bash

# Grid Search Script for MPC Performance Optimization
# This script performs a grid search over performance-related parameters

set -e  # Exit on error

# Output files
RESULTS_FILE="grid_search_results.csv"
LOG_DIR="grid_search_logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Create log directory
mkdir -p "$LOG_DIR"

# Define parameter ranges for grid search
# Adjust these ranges based on your system capabilities
SESSION_PER_REQUEST_VALUES=(1 2 4 8)
CONNECTION_PARALLELISM_VALUES=(1 2 4)
REQUEST_PARALLELISM_VALUES=(1 2 4)
CPU_TO_NETWORK_RATIO_VALUES=(1 2 4 8)

# Configuration for the MPC application
PARTY_INDEX=${PARTY_INDEX:-0}

# Initialize results CSV file
echo "timestamp,session_per_request,connection_parallelism,request_parallelism,cpu_to_network_ratio,mean_time_us,min_time_us,max_time_us,throughput_mcomp_s,status" > "$RESULTS_FILE"

# Build the application
echo "Building application..."
cargo build --release
echo "Build complete."
echo ""

# Counter for progress tracking
total_combinations=$((${#SESSION_PER_REQUEST_VALUES[@]} * ${#CONNECTION_PARALLELISM_VALUES[@]} * ${#REQUEST_PARALLELISM_VALUES[@]} * ${#CPU_TO_NETWORK_RATIO_VALUES[@]}))
current=0

echo "Starting grid search with $total_combinations parameter combinations"
echo "Results will be saved to: $RESULTS_FILE"
echo "Logs will be saved to: $LOG_DIR/"
echo ""

# Grid search loop
for spr in "${SESSION_PER_REQUEST_VALUES[@]}"; do
  for cp in "${CONNECTION_PARALLELISM_VALUES[@]}"; do
    for rp in "${REQUEST_PARALLELISM_VALUES[@]}"; do
      for cnr in "${CPU_TO_NETWORK_RATIO_VALUES[@]}"; do
        current=$((current + 1))

        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "Progress: $current/$total_combinations"
        echo "Parameters:"
        echo "  SESSION_PER_REQUEST:      $spr"
        echo "  CONNECTION_PARALLELISM:   $cp"
        echo "  REQUEST_PARALLELISM:      $rp"
        echo "  CPU_TO_NETWORK_RATIO:     $cnr"
        echo ""

        # Create unique log file for this run
        log_file="$LOG_DIR/run_${current}_spr${spr}_cp${cp}_rp${rp}_cnr${cnr}.log"

        # Run the application with current parameters
        run_start=$(date +%s)
        status="success"

        if ./target/release/vector-mpc-poc \
          --party-index "$PARTY_INDEX" \
          --session-per-request "$spr" \
          --connection-parallelism "$cp" \
          --request-parallelism "$rp" \
          --cpu-to-network-ratio "$cnr" \
          > "$log_file" 2>&1; then

          # Extract metrics from log file
          mean_time=$(grep "Total:" "$log_file" | grep -oE "mean=[0-9.]+[a-z]+" | grep -oE "[0-9.]+" || echo "0")
          min_time=$(grep "Total:" "$log_file" | grep -oE "min=[0-9.]+[a-z]+" | grep -oE "[0-9.]+" || echo "0")
          max_time=$(grep "Total:" "$log_file" | grep -oE "max=[0-9.]+[a-z]+" | grep -oE "[0-9.]+" || echo "0")

          # Extract throughput (M comp/s) - get the last one
          throughput=$(grep -oE "[0-9.]+M comp/s" "$log_file" | tail -1 | grep -oE "[0-9.]+" || echo "0")

          # Convert time units to microseconds if needed (the log shows Duration format)
          # For now, we'll store the raw values from the histogram

          echo "  Mean time:     ${mean_time}µs"
          echo "  Min time:      ${min_time}µs"
          echo "  Max time:      ${max_time}µs"
          echo "  Throughput:    ${throughput} M comp/s"
          echo "  Status:        ✓ SUCCESS"
        else
          status="failed"
          mean_time="N/A"
          min_time="N/A"
          max_time="N/A"
          throughput="N/A"

          echo "  Status:        ✗ FAILED"
        fi

        run_end=$(date +%s)
        run_duration=$((run_end - run_start))
        echo "  Run duration:  ${run_duration}s"
        echo ""

        # Append results to CSV
        echo "${TIMESTAMP},${spr},${cp},${rp},${cnr},${mean_time},${min_time},${max_time},${throughput},${status}" >> "$RESULTS_FILE"

        # Sleep briefly between runs to let system stabilize
        sleep 2
      done
    done
  done
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Grid search complete!"
echo ""
echo "Results saved to: $RESULTS_FILE"
echo "Logs saved to: $LOG_DIR/"
echo ""

# Generate summary report
echo "Generating summary report..."

echo ""
echo "Top 5 configurations by throughput:"
echo "───────────────────────────────────────────────────────────────"
{
  head -1 "$RESULTS_FILE"
  tail -n +2 "$RESULTS_FILE" | grep "success" | sort -t',' -k9 -nr | head -5
} | column -t -s','

echo ""
echo "Top 5 configurations by mean latency (lowest):"
echo "───────────────────────────────────────────────────────────────"
{
  head -1 "$RESULTS_FILE"
  tail -n +2 "$RESULTS_FILE" | grep "success" | sort -t',' -k6 -n | head -5
} | column -t -s','

echo ""
echo "Failed runs:"
failed_count=$(grep -c "failed" "$RESULTS_FILE" || echo "0")
echo "Total failed: $failed_count"
if [ "$failed_count" -gt 0 ]; then
  grep "failed" "$RESULTS_FILE" | column -t -s','
fi

echo ""
echo "Done!"
