#!/bin/sh
# cgroup-v2 sampler for a running OpenClaw/OpenShell container.
# Output: epoch_ns,cpu_usage_usec,memory_bytes
set -eu

interval="${CGROUP_SAMPLE_INTERVAL:-0.100}"
trap 'exit 0' INT TERM EXIT
printf '%s\n' 'epoch_ns,cpu_usage_usec,memory_bytes'

while :; do
  cpu=0
  while read -r key value rest; do
    if [ "$key" = "usage_usec" ]; then
      cpu="$value"
      break
    fi
  done < /sys/fs/cgroup/cpu.stat
  memory="$(cat /sys/fs/cgroup/memory.current)"
  printf '%s,%s,%s\n' "$(date +%s%N)" "$cpu" "$memory"
  sleep "$interval"
done
