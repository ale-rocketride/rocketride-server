# Telemetry overhead and compatibility checks

What telemetry (`RR_OTEL_ENABLED`) adds to the server, measured, and a check that
clients see the same events with it. Run everything from the server's
`dist/server` directory with the engine's Python (`./engine <script>`).

| Script | What it measures |
|---|---|
| `bench_deps.py` | Per call, in EaaS: a WebSocket request and a database call, before and after the dependency telemetry (sqlite in memory: the worst case, the database adds nothing of its own). The per-call scripts count the measurements queued for the otel-deps thread inside the time measured |
| `bench_trace.py` | Per call, in EaaS: a component's enter and leave (the engine's trace of every node call), with the pass bookkeeping and the duration histogram |
| `bench_relay.py` | Per call, in a run's process: a node handler call with the relay on, and one relayed dependency call |
| `load.py`, `ab.sh`, `mock_llm.py` | End to end, N concurrent clients: `text` (2 KB objects, webhook → parse → response, no outside call) or `llm` (chat → LLM node → an OpenAI-compatible mock with a fixed delay, so every request makes an HTTP call from the node). Request latency p50/p95 and the CPU of the EaaS process and its run processes; servers taken in rotating order |
| `summarize.py` | Medians of an `ab.sh` log, each server's difference from the mean of the "off" servers, the noise floor (how far the "off" servers are apart) and CPU: `python3 summarize.py LOG 5567,5571` |
| `event_monitor_ab.py` | What an Event Monitor client receives for the same runs (every event type), to compare two servers (e.g. `RR_LOG_FORMAT` unset vs `json`) |

## End to end

```
cd dist/server
T=../../tools/telemetry_overhead
tail -f /dev/null | ./engine $T/mock_llm.py 18089 20 &                  # LLM mock, 20 ms per answer
tail -f /dev/null | ./engine ai/eaas.py --port 5567 --base_port 33000 & # off
tail -f /dev/null | ./engine ai/eaas.py --port 5571 --base_port 36000 & # off again: A/A, the noise floor
tail -f /dev/null | RR_OTEL_ENABLED=true RR_OTEL_TRACES_SAMPLE_RATIO=0 \
  OTEL_EXPORTER_OTLP_ENDPOINT=http://<collector>:4318 \
  ./engine ai/eaas.py --port 5568 --base_port 32000 &                   # on, production settings
sh $T/ab.sh "300 4 llm 20" 5 5567 5571 5568    # requests, clients, pipeline, warm-up; 5 rounds
sh $T/ab.sh "300 4 text 20" 5 5567 5571 5568
```

`tail -f /dev/null |` keeps stdin open: with stdin at end of file (`/dev/null`,
a closed pipe) the engine's stdin watcher spins a core, which would swamp the
difference measured.
The difference between the two "off" servers is the noise: telemetry's cost is
what "on" adds beyond it.

Keep the output (`... > <date>-ab.txt`, outside the repository) and summarize it
with `summarize.py`. Add a server with `RR_OTEL_INSTRUMENT=none` to see the dependency recording apart,
or one with `OTEL_EXPORTER_OTLP_ENDPOINT` pointing nowhere to see a collector that
is down. Compare rounds, not single numbers: the machine's load moves both.

## Event Monitor

```
./engine ../../tools/telemetry_overhead/event_monitor_ab.py 5567 > text.json
./engine ../../tools/telemetry_overhead/event_monitor_ab.py 5568 > json.json
```

Compare event types, fields and output lines; run each twice to see what varies
by timing alone; an event carrying the engine's pass id (`tid`) is marked. Run it
against a server on the previous engine build to compare engines.

## Reference figures

Measured once, on one machine (8-core macOS laptop, OrbStack, engine 3.6.0,
OpenTelemetry SDK 1.45.2, collector reached through a `kubectl port-forward`),
telemetry on with production settings (`RR_OTEL_TRACES_SAMPLE_RATIO=0`), against
the same server with telemetry off:

- End to end, four clients: `llm` p50 +2.9 ms (2 %; 7 % more CPU); `text`, the
  worst case (a saturated event loop, almost no work per object), p50 +4.4 ms
  (15 %; 10 % more CPU).
- Per call: 11–12 µs per component enter or leave in EaaS, 22–26 µs per
  WebSocket request, 39–41 µs per database call (sqlite in memory, the worst
  case), 2.2–2.4 µs per node handler call in a run's process with the relay on.
- Telemetry off: within or just above the noise floor against the engine build
  without telemetry.

These depend on the machine and its load (on a busier moment of the same
machine every per-call figure was about 1.5 times these). Measure on your own
hardware with the scripts above; the raw results are not kept in the repository.
