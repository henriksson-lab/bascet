#!/bin/bash
  declare -A total_cpu
  declare -A sample_count

  trap 'exit 0' INT

  while true; do
    clear
    echo "Thread CPU Usage - Running Average Since Start - $(date)"
    echo "=========================================="

    # Collect current sample
    while read pcpu comm; do
      # Remove @number suffix to get thread type
      name=${comm/@[0-9]*/}

      # Accumulate
      total_cpu[$name]=$(echo "${total_cpu[$name]:-0} + $pcpu" | bc)
      ((sample_count[$name]++))
    done < <(ps H -p $(pgrep bascet) -o pcpu,comm --no-headers 2>/dev/null)

    # Display averages
    for name in "${!total_cpu[@]}"; do
      avg=$(echo "scale=2; ${total_cpu[$name]} / ${sample_count[$name]}" | bc)
      printf "%-15s Avg: %6.2f%%  (samples=%d)\n" "$name" "$avg" "${sample_count[$name]}"
    done | sort -k3 -rn

    sleep 0.1
done