Run a stress test scenario against real R2 with live monitoring.

## What to do

1. Source `.env` for R2 credentials.

2. Pick the scenario. The argument `$ARGUMENTS` names the pytest test class (default: `TestThroughputModerate`). Valid options: `TestThroughputModerate`, `TestHighContention`, `TestHeartbeatTimeout`, `TestMultiQueueIsolation`, `TestCompaction`, `TestBrokerRestart`.

3. Run the test in the background:
   ```
   set -a && source .env && set +a && uv run pytest tests/stress/test_scenarios.py::CLASS -v -s --log-cli-level=INFO -m stress
   ```

4. Immediately find the server log file by looking for the most recent `*.server.log` in the system temp directory.

5. **Every 30 seconds**, produce a live status report with:
   - **Server flush health**: grep the server log for `Flush slow` entries and count them. Show the slowest flush time.
   - **S3 write timing**: grep for `s3 write` debug lines, compute the p50/p99 write latency so far.
   - **Heartbeat timeouts**: grep for `timeout` (case insensitive) — count how many tasks were re-queued.
   - **Worker progress**: count lines in the consumer/producer JSONL event files in the pytest tmp directory (`/private/tmp/pytest-*`). Show pushed vs claimed vs finished counts.
   - **Hedging activity**: grep for `hedged write landed` — count how many hedges fired and saved latency.
   - **Elapsed wall clock time**.

   Format each report as a compact markdown table. Keep checking until the test completes.

6. When the test finishes, give a **final summary**:
   - Pass/fail status
   - Full metrics line from the test output (the JSON blob with p50/p95/p99 latencies and throughput)
   - Total hedges fired
   - Total heartbeat timeouts
   - Slowest S3 write
   - Any anomalies (flushes >5s, requeue count >0, etc.)

## Important

- Use `run_in_background` for the test command so you can monitor while it runs.
- Use `block: false` when checking task output so you don't hang waiting.
- Don't sleep longer than 30s between checks — the user wants live updates.
- If the test takes more than 10 minutes, flag it as likely hung.
