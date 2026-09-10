import fs from "node:fs";

const intervalMs = Number(process.env.CGROUP_SAMPLE_INTERVAL_MS || 100);
const durationMs = Number(process.env.CGROUP_SAMPLE_DURATION_MS || 0);
const cpuPath = "/sys/fs/cgroup/cpu.stat";
const memoryPath = "/sys/fs/cgroup/memory.current";
const startMonotonicNs = process.hrtime.bigint();
const startEpochNs = BigInt(Date.now()) * 1000000n;

function readSample() {
  const cpuLine = fs.readFileSync(cpuPath, "utf8").split("\n").find((line) => line.startsWith("usage_usec"));
  return {
    epoch_ns: startEpochNs + (process.hrtime.bigint() - startMonotonicNs),
    monotonic_ns: process.hrtime.bigint() - startMonotonicNs,
    cpu_usage_usec: Number(cpuLine?.trim().split(/\s+/)[1] || 0),
    memory_bytes: Number(fs.readFileSync(memoryPath, "utf8").trim()),
  };
}

process.stdout.write("epoch_ns,monotonic_ns,cpu_usage_usec,memory_bytes\n");
let active = true;
const stop = () => { active = false; };
process.on("SIGTERM", stop);
process.on("SIGINT", stop);

while (active) {
  const sample = readSample();
  process.stdout.write(`${sample.epoch_ns},${sample.monotonic_ns},${sample.cpu_usage_usec},${sample.memory_bytes}\n`);
  if (durationMs > 0 && Number(sample.monotonic_ns) / 1e6 >= durationMs) break;
  await new Promise((resolve) => setTimeout(resolve, intervalMs));
}
