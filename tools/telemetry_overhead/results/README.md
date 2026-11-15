# Raw results

What `ab.sh` and the per-call scripts printed, unedited. `summarize.py LOG OFF_PORTS`
recomputes medians and differences from an `ab.sh` log. Every server ran in the
SaaS dev container with stdin held open; "on" is `RR_OTEL_ENABLED=true` with
`RR_OTEL_TRACES_SAMPLE_RATIO=0` unless stated.

| File | Servers (port: what) | Code |
|---|---|---|
| `2026-11-09-ab.txt` | 5580: engine build before the pass ids, off; 5567, 5571: off; 5568: on | Final |
| `2026-11-09-per-call.txt` | `bench_deps.py`, `bench_trace.py`, `bench_relay.py`, three runs | Final, before threads kept the pass |
| `2026-11-09-relay-threads.txt` | `bench_relay.py`, three runs | The pass scope in the OpenTelemetry context |
| `2026-11-08-instrument-split.txt` | 5567: off; 5569: on with `RR_OTEL_INSTRUMENT=none`; 5568: on (the first lines list the server processes) | Previous engine changes, metrics recorded inline |
| `2026-11-08-collector-refused.txt` | 5567: off; 5570: on, collector `127.0.0.1:9` (refused); 5568: on | Trial: metrics recorded on a thread every 1 s |
| `2026-11-08-collector-never-answers.txt` | 5567, 5571: off; 5568: on; 5570: on, collector `10.255.255.1` (never answers). Round 1 was a warm-up (one load failed while the servers started) | Trial, as above |
| `2026-11-08-trial-drain-interval.txt` | 5567, 5571: off; 5568: on, recorded every 1 s; 5569: on, every 0.1 s | Trial |
| `2026-11-08-trial-busy-machine.txt` | 5567, 5571: off; 5568: on, recorded every 0.1 s; the machine got busy during `text` | Trial |
