#!/bin/bash

# Analysis Script for Grid Search Results
# This script analyzes the results from grid_search.sh and provides insights

RESULTS_FILE="${1:-grid_search_results.csv}"

if [ ! -f "$RESULTS_FILE" ]; then
  echo "Error: Results file '$RESULTS_FILE' not found!"
  echo "Usage: $0 [results_file.csv]"
  exit 1
fi

echo "═══════════════════════════════════════════════════════════════"
echo "Grid Search Results Analysis"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# Count total runs
total_runs=$(tail -n +2 "$RESULTS_FILE" | wc -l)
successful_runs=$(grep -c "success" "$RESULTS_FILE" || echo "0")
failed_runs=$(grep -c "failed" "$RESULTS_FILE" || echo "0")

echo "Summary Statistics:"
echo "  Total runs:       $total_runs"
echo "  Successful runs:  $successful_runs"
echo "  Failed runs:      $failed_runs"
echo ""

if [ "$successful_runs" -eq 0 ]; then
  echo "No successful runs to analyze!"
  exit 0
fi

echo "═══════════════════════════════════════════════════════════════"
echo "Top 10 Configurations by Throughput (M comp/s)"
echo "═══════════════════════════════════════════════════════════════"
{
  echo "SPR,CP,RP,CNR,Throughput,Mean_Time,Status"
  tail -n +2 "$RESULTS_FILE" | grep "success" | \
    awk -F',' '{print $2","$3","$4","$5","$9","$6",success"}' | \
    sort -t',' -k5 -nr | head -10
} | column -t -s','
echo ""

echo "═══════════════════════════════════════════════════════════════"
echo "Top 10 Configurations by Lowest Mean Latency (µs)"
echo "═══════════════════════════════════════════════════════════════"
{
  echo "SPR,CP,RP,CNR,Mean_Time,Throughput,Status"
  tail -n +2 "$RESULTS_FILE" | grep "success" | \
    awk -F',' '{print $2","$3","$4","$5","$6","$9",success"}' | \
    sort -t',' -k5 -n | head -10
} | column -t -s','
echo ""

echo "═══════════════════════════════════════════════════════════════"
echo "Parameter Impact Analysis"
echo "═══════════════════════════════════════════════════════════════"

# Analyze impact of each parameter
echo ""
echo "Average throughput by SESSION_PER_REQUEST:"
echo "  SPR | Avg Throughput (M comp/s) | Count"
tail -n +2 "$RESULTS_FILE" | grep "success" | \
  awk -F',' '{spr=$2; thr=$9; sum[spr]+=thr; count[spr]++}
             END {for (s in sum) printf "  %3s | %25.2f | %5d\n", s, sum[s]/count[s], count[s]}' | \
  sort -n -k1

echo ""
echo "Average throughput by CONNECTION_PARALLELISM:"
echo "  CP  | Avg Throughput (M comp/s) | Count"
tail -n +2 "$RESULTS_FILE" | grep "success" | \
  awk -F',' '{cp=$3; thr=$9; sum[cp]+=thr; count[cp]++}
             END {for (c in sum) printf "  %3s | %25.2f | %5d\n", c, sum[c]/count[c], count[c]}' | \
  sort -n -k1

echo ""
echo "Average throughput by REQUEST_PARALLELISM:"
echo "  RP  | Avg Throughput (M comp/s) | Count"
tail -n +2 "$RESULTS_FILE" | grep "success" | \
  awk -F',' '{rp=$4; thr=$9; sum[rp]+=thr; count[rp]++}
             END {for (r in sum) printf "  %3s | %25.2f | %5d\n", r, sum[r]/count[r], count[r]}' | \
  sort -n -k1

echo ""
echo "Average throughput by CPU_TO_NETWORK_RATIO:"
echo "  CNR | Avg Throughput (M comp/s) | Count"
tail -n +2 "$RESULTS_FILE" | grep "success" | \
  awk -F',' '{cnr=$5; thr=$9; sum[cnr]+=thr; count[cnr]++}
             END {for (c in sum) printf "  %3s | %25.2f | %5d\n", c, sum[c]/count[c], count[c]}' | \
  sort -n -k1

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Recommended Configuration"
echo "═══════════════════════════════════════════════════════════════"

best_config=$(tail -n +2 "$RESULTS_FILE" | grep "success" | sort -t',' -k9 -nr | head -1)

if [ -n "$best_config" ]; then
  spr=$(echo "$best_config" | cut -d',' -f2)
  cp=$(echo "$best_config" | cut -d',' -f3)
  rp=$(echo "$best_config" | cut -d',' -f4)
  cnr=$(echo "$best_config" | cut -d',' -f5)
  mean=$(echo "$best_config" | cut -d',' -f6)
  throughput=$(echo "$best_config" | cut -d',' -f9)

  echo "Best configuration for maximum throughput:"
  echo "  SESSION_PER_REQUEST:      $spr"
  echo "  CONNECTION_PARALLELISM:   $cp"
  echo "  REQUEST_PARALLELISM:      $rp"
  echo "  CPU_TO_NETWORK_RATIO:     $cnr"
  echo ""
  echo "Performance:"
  echo "  Throughput:   $throughput M comp/s"
  echo "  Mean latency: $mean µs"
  echo ""
  echo "Command to use these parameters:"
  echo "  export SESSION_PER_REQUEST=$spr"
  echo "  export CONNECTION_PARALLELISM=$cp"
  echo "  export REQUEST_PARALLELISM=$rp"
  echo "  export CPU_TO_NETWORK_RATIO=$cnr"
  echo ""
  echo "Or run with:"
  echo "  ./target/release/mpc-dummy --session-per-request $spr \\"
  echo "    --connection-parallelism $cp --request-parallelism $rp \\"
  echo "    --cpu-to-network-ratio $cnr \\"
  echo "    --party-index <index> --addresses <addresses>"
fi

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "Analysis complete!"
echo "═══════════════════════════════════════════════════════════════"
