# Results

Local: macOS, OrbStack, 8 cores, the SaaS dev container; engine 3.6.0; OpenTelemetry
SDK 1.45.2; collector: Alloy → Mimir/Tempo/Loki on OrbStack, reached through a
`kubectl port-forward`. Telemetry "on" means production settings
(`RR_OTEL_TRACES_SAMPLE_RATIO=0`: failed and slow traces only), with dependency
recording, relay and pass ids, metrics exported every 15 s (production: 60 s).
The raw output of each run is in `results/`; `summarize.py` recomputes every
end-to-end figure below from it.

Every server, and every load client, ran with stdin held open (README): with
stdin at end of file the engine's stdin watcher spins a core, which swamped
earlier measurements; those were discarded. This machine's speed varied during
the sessions (other load in the OrbStack VM, and the CPU itself): compare
figures taken together, never across tables.

## End to end, 2026-11-09 (`results/2026-11-09-ab.txt`)

`ab.sh`: 4 clients × 300 requests after 20 of warm-up, 6 rounds, the servers in
rotating order. `llm`: chat → LLM node → `mock_llm.py` answering in 20 ms (every
request makes an HTTP call from the node). `text`: webhook → parse → response,
2 KB objects, no outside call; with 4 clients this saturates EaaS's event loop,
the worst case for telemetry's share.

Four servers: this engine with telemetry off, twice (5567, 5571); this engine
with telemetry on (5568); and the engine build from before the pass ids, with
telemetry off (5580). Each figure is the median of the six rounds. **Added** is
a server's median minus the baseline, **the mean of the two "off" servers of
this engine**; how far those two are apart is the noise floor. CPU: ticks of the
server and its run processes per round (all servers serve the same requests).
`python3 summarize.py results/2026-11-09-ab.txt 5567,5571` prints this table.

| | p50 ms | p95 ms | CPU ticks |
|---|---|---|---|
| **llm**, baseline (5567, 5571) | 138.0 | 146.2 | 8370 |
| noise floor (5567 vs 5571) | 0.2 | 0.2 | 30 |
| On (5568) | 140.9: **+2.9 ms (2%)** | 151.1: +4.9 ms (3%) | 8926: **+7%** |
| Previous engine, off (5580) | 138.3: +0.3 | 146.3: +0.1 | 8542: +2% |
| **text**, baseline (5567, 5571) | 30.1 | 54.2 | 8228 |
| noise floor (5567 vs 5571) | 1.1 | 3.5 | 610 |
| On (5568) | 34.5: **+4.4 ms (15%)** | 61.6: +7.4 ms (14%) | 9047: +10% (marginal: the noise floor is 7%) |
| Previous engine, off (5580) | 31.0: +0.9 | 56.8: +2.7 | 8430: +2% |

- **Telemetry off costs nothing measurable.** The previous engine build with
  telemetry off is no faster than this one (within the noise floor or just
  above it, and in CPU within the `text` floor): with telemetry off the engine
  sends no pass id and the run's process gets no telemetry argument.
- **On**, a request that calls an LLM took 2.9 ms longer (2%), with 7% more CPU.
  The light pipeline, its loop saturated, took 4.4 ms longer (15%), 10% more CPU.
  Earlier sessions gave the same picture: `llm` +2.3 to +3.5 ms, `text` +3.1 to
  +7.2 ms, larger when the machine was slower or busier: what telemetry adds is
  CPU, so it shows most when CPU is scarce.
- `text` is the worst case on purpose: almost no work per object, an event loop
  kept busy by four clients, and 24 timed enters and leaves per object. A
  pipeline whose nodes do real work (an LLM, a database, an embedding) pays the
  same fixed amount per object, a small share of it, as `llm` shows.

Load average: 0.9 when the `llm` rounds began, 3.7 when they ended, 8.8 at the
end of the `text` rounds.

## Per call, 2026-11-09 (`results/2026-11-09-per-call.txt`)

Each figure is the whole CPU cost, including what is queued for the otel-deps
thread (dependency calls) and recorded there. Three runs each, the machine
quiet:

| | Off | On | Added |
|---|---|---|---|
| WebSocket request, empty handler (`bench_deps.py`) | 37.0–38.8 µs | 60.5–62.9 µs | 22–26 µs |
| Database call outside a request (sqlite in memory: the worst case) | 32.0–32.4 µs | 71.6–73.3 µs | 39–41 µs |
| Request with 1 database call | 98.9–102.9 µs | 165.0–171.8 µs | 65–69 µs |
| A component's enter or leave, in EaaS (`bench_trace.py`: a new pass id each pass) | | | 11.2–11.5 µs |
| Node handler call in a run's process, relay on (`bench_relay.py`) | 0.07 µs | 1.55–1.66 µs | 1.5–1.6 µs |
| One relayed dependency call (queued; written in batches) | | | 3.8–3.9 µs |

On a slower moment of the same machine (2026-11-08, "off" request 55–58 µs)
every added figure was about 1.5 times these.

Since then the pass's scope moved into the OpenTelemetry context, so the work a
node hands to a thread or an executor keeps its pass: a node handler call now
costs 2.2–2.4 µs instead of 1.5–1.6 (`results/2026-11-09-relay-threads.txt`); a
relayed call is unchanged.

An earlier version of these figures (database call +23 µs) left out the
recording of dependency calls, which happens on the otel-deps thread once a
second: it rarely ran inside a measured round. The scripts now record the queue
inside the time measured. The first design (standard SQLAlchemy
instrumentation, a live span per request) added 133 µs per database call and
63 µs per request; replaced.

## Where the time goes (2026-11-08, previous engine changes; `results/2026-11-08-instrument-split.txt`)

Five rounds each, without an A/A pair, so read the differences against the
noise floors above:

| | Off | On, `RR_OTEL_INSTRUMENT=none` (EaaS hooks only, no relay) | On |
|---|---|---|---|
| llm p50 | 137.2 | 138.6 (+1.4) | 139.5 (+2.3) |
| text p50 | 28.4 | 33.0 (+4.6) | 31.5 (+3.1) |

- On `text` the relay adds nothing measurable (no dependency call; this run had
  no A/A pair, and "none" came out above "on"): what remains is EaaS's hooks,
  the component timing and the per-request hooks, about 24 enters/leaves and 3
  requests per object, a few hundred µs of
  CPU per object (table above), on a saturated loop where each microsecond
  delays the next request.
- That CPU does not account for all of the latency added: 24 × 11.5 µs + 3 ×
  24 µs is about 0.35 ms per object, about 1.4 ms queued behind four clients,
  against 4.4 ms measured. Sampling every
  thread's stack in EaaS every 5 ms under `llm` load, telemetry's own functions
  were under 1% of the samples where the main thread was busy, and the main
  thread was busy about 20% more with telemetry on, the difference in socket
  write calls (to the clients and to the run log), not in Python. Likely, but
  not shown: more system load in the same VM while telemetry is on (the export
  through the port-forward, the collector's pods, which share these cores). An
  environment with the collector on other hosts would tell; until then this
  part is unexplained.

## Collector down (2026-11-08; `results/2026-11-08-collector-*.txt`)

Same load, a server whose collector refuses connections (`127.0.0.1:9`) or never
answers (`10.255.255.1`, the worst case: every export waits for its timeout).

| Run | Off p50 | On, collector up | On, refused | On, never answers |
|---|---|---|---|---|
| llm, 3 rounds | 136.3–137.0 | 138.5–139.1 | 138.2–143.2 | |
| llm, rounds 2–5 of 5 | 137.6–138.3 | 140.9 | | 139.5 |
| text, rounds 2–5 of 5 | 29.2–30.4 | 31.6 | | 32.2 |

Every request answered; the refused collector's slowest round (143.2 ms) was
4 ms above the collector up, its other rounds within it. These runs (and the
Event Monitor engine comparison) were taken during the trial under *Tried and
not kept*, which changes neither the export nor the events. The export fails on
its own thread: the server's log has the exporter's error at most once a minute
per signal (with a count of the suppressed ones), nothing else. Model server:
p50 1.28 → 0.69 ms and p95 2.76 → 1.31 ms with its box's collector stopped (no
slower).

## Event Monitor (2026-11-08)

What a client subscribed to every event type receives for a run that parses text
and a run that fails in a node (`event_monitor_ab.py`):

| Compared | Result |
|---|---|
| Server log as text vs `RR_LOG_FORMAT=json`, three runs each | Same event types, fields and output lines |
| Engine before the pass trace id (03:05 build) vs after, telemetry on in both, two runs each | Same event types and fields, same output lines; no event carries `tid` in either |

What varies from run to run, in both modes and with both engines: the number of
periodic `apaevt_status_update` events, an asyncio warning ("Task was destroyed
but it is pending") at the run's process exit, and an `apaevt_status_user` event
(the engine's `>USR*` line, which the change does not touch) seen once in four
runs.

## Tried and not kept (2026-11-08; `results/2026-11-08-trial-*.txt`)

Recording the component, request, object and token metrics on the otel-deps
thread instead of the event loop (as dependency calls are). On the loop a
component's enter or leave then cost 6.7 µs instead of 12.5 µs, but 16–17 µs of
CPU in all (the queue, and a second thread taking the GIL). End to end it made
no difference beyond the noise (`text` p50 +0.6 to +3.4 ms against +3.1 ms; with
a 1 s drain, ~20 ms of recording at once raised `text` p95 to 63.9 ms against
52–58 ms off; with a 0.1 s drain, p95 like off), and on a busy machine the extra
CPU showed (`text` +7 ms, against 41–42 ms off). Recording stays on the caller's
path. If a light pipeline at a high rate needs the per-component cost lower, the
lever is one record per pass instead of one per enter and leave.
