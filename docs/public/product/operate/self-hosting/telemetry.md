---
title: Telemetry (OpenTelemetry)
---

# Telemetry with OpenTelemetry

The engine can export metrics and traces with [OpenTelemetry](https://opentelemetry.io/)
(OTLP over HTTP) to any compatible backend: an OpenTelemetry Collector, Grafana
Alloy, Grafana Cloud, Datadog, Honeycomb and so on. It is **off by default** and
changes nothing until you turn it on.

The telemetry comes from what the server already does: the task events that
monitoring clients receive, the component enter/leave that the engine reports
for every pipeline at every trace level, each run's outcome, and the result of
each WebSocket request. Pipelines and nodes need no code of their own, so every
node is covered, including ones you add.

This is separate from the [`rocketride otel` bridge](/clients/python/otel-bridge).
The bridge is an external client that subscribes to a server's events over the
WebSocket. This page covers the telemetry built into the server.

## Turn it on

```bash
RR_OTEL_ENABLED=true \
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 \
OTEL_RESOURCE_ATTRIBUTES=deployment.environment.name=production \
./engine ai/eaas.py
```

Telemetry needs the OpenTelemetry SDK next to the engine. Install it once, for
example in your image build:

```bash
./engine -c "from ai.common.telemetry import install_sdk; install_sdk()"
```

This installs `ai/common/telemetry/requirements.txt`: the SDK, the OTLP/HTTP
exporter and the instrumentations of dependency calls, at fixed versions that
are resolved together with every node's dependencies. Nothing installs them
while telemetry is off.

Without it, telemetry stays off and the server logs one warning saying so. With
`RR_OTEL_AUTO_INSTALL=true` the server installs it itself, in the background, and
turns telemetry on when it is ready. That is convenient on a laptop but not
recommended for production: while the install runs, the engine's dependency
installer is busy, so task starts that need packages wait for it.

### Configuration

The standard [OpenTelemetry SDK variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/)
apply. The `RR_OTEL_*` variables exist only for what has no standard.

| Variable | Default | Meaning |
|---|---|---|
| `RR_OTEL_ENABLED` | off | `true` turns telemetry on |
| `OTEL_SDK_DISABLED` | | `true` keeps it off, whatever `RR_OTEL_ENABLED` says |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `_HEADERS`, `_TIMEOUT`, `_COMPRESSION`, `_CERTIFICATE` (and the `_TRACES_` / `_METRICS_` variants) | `http://localhost:4318` | Where and how to export (OTLP/HTTP) |
| `OTEL_SERVICE_NAME` | `rocketride-eaas` | `service.name` |
| `OTEL_RESOURCE_ATTRIBUTES` | | e.g. `deployment.environment.name=production` |
| `OTEL_METRIC_EXPORT_INTERVAL` | `60000` | Metric export interval (ms) |
| `OTEL_BSP_*` | SDK defaults | Span batching |
| `OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION` | exponential | Set `explicit_bucket_histogram` for a backend without exponential (native) histograms |
| `RR_OTEL_TRACES_SAMPLE_RATIO` | `1.0` | Share of traces that did not fail kept: pipeline passes, runs and traced requests. Failed ones are always kept, and so are slow ones (the `RR_OTEL_*SLOW*` rows). `0` exports errors only |
| `RR_OTEL_TRACES_SLOW_SECONDS` | `60` | A pipeline pass at least this long is kept (`0`: off) |
| `RR_OTEL_SLOW_RUN_SECONDS` | `600` | A run (not a deploy, which stays up) at least this long is kept (`0`: off) |
| `RR_OTEL_SLOW_REQUEST_SECONDS` | `2` | A request, or a dependency call made outside any request, at least this long is kept (`0`: off) |
| `RR_OTEL_INSTRUMENT` | `db,redis,http,aws,grpc,model` | The dependency calls traced and measured, the server's and the nodes': `db` (SQLAlchemy, and without it psycopg, psycopg2, pymongo, neo4j), `redis` (redis-py), `http` (httpx, requests, aiohttp, urllib3), `aws` (botocore), `grpc` (gRPC clients: Qdrant, Weaviate, Milvus, Google Cloud), `model` (the model server). `none` for none. All but `redis`, `model`, SQLAlchemy and neo4j need the instrumentation packages (see *Dependency calls*) |
| `RR_OTEL_SLOW_REQUEST_EXCLUDE` | `rrext_cprofile_*` | Request names (shell patterns, comma-separated) that are slow by design and never kept for being slow |
| `RR_OTEL_SLOW_TRACES_PER_MINUTE` | `100` | Most slow traces kept per minute by the process; one run, or one org, gets at most a tenth of it (at least 1). The rest are counted in `rocketride.telemetry.dropped` as `slow_rate_limited`. Failed traces are never limited |
| `RR_OTEL_COMPONENT_SPANS` | `200` | Maximum component spans per pipeline trace (`0`: none; at most `1000`) |
| `RR_OTEL_ORG_ATTRIBUTE` | `true` | `false` leaves the org id off metrics and spans |
| `RR_OTEL_ORG_CARDINALITY` | `2000` | Distinct org ids the process attributes, across all metrics; later ones are `_other` (counted in `rocketride.telemetry.dropped`) |
| `RR_OTEL_AUTO_INSTALL` | off | `true` installs a missing SDK at startup, in the background (see above) |
| `RR_OTEL_SHUTDOWN_TIMEOUT` | `3` | Seconds a shutdown may wait to flush |
| `RR_LOG_FORMAT` | text | `json` writes the server's web server (uvicorn) and telemetry log lines, and the run error records, as one JSON object per line (`ts`, `level`, `logger`, `msg`, ids, `exc`). Unset, the output is as before |
| `RR_OTEL_STALE_CALL_SECONDS` | off | A node call open this long is taken as a lost leave: its pass is exported unfinished and the pipe traces again. Leave it off unless calls lose their leave: the outermost call of a pass stays open for the whole pass, so a longer pass would be split; set it above `RR_OTEL_STALL_LOG_SECONDS`, or it resets a stalled pass |
| `RR_OTEL_OVERDUE_REQUEST_SECONDS` | `300` | A WebSocket request still being handled after this long writes one error line (`rocketride.request.overdue`) and its trace is kept when it ends; `0`: never |
| `RR_OTEL_STALL_LOG_SECONDS` | `900` | A pass with a call open and no enter or leave for this long writes one error line naming its run (`rocketride.pass.stalled`); `0`: never |
| `RR_OTEL_STALL_EXEMPT_TYPES` | none | Node types (fnmatch patterns, comma-separated) that wait for a person, such as an approval: never reported as stalled |
| `ROCKETRIDE_OTEL_ALLOW_INSECURE` | off | Allow credential headers over plain `http://` to a non-loopback endpoint |

Telemetry refuses to send credential headers (for example `Authorization` or
`x-api-key` in `OTEL_EXPORTER_OTLP_HEADERS`) over plain `http://` to anything but
loopback. Use `https://`, or set `ROCKETRIDE_OTEL_ALLOW_INSECURE=1` on a trusted
network. The `rocketride otel` bridge applies the same check.

Task subprocesses never receive `OTEL_*` variables (the engine passes them a fixed
list, unless you add them to `RR_SUBPROCESS_ENV`), so a node library does not start
exporting because the server does.

Not supported: `OTEL_EXPORTER_OTLP_PROTOCOL=grpc` (the server logs a warning and
uses OTLP/HTTP), `OTEL_TRACES_SAMPLER` (sampling is the `RR_OTEL_TRACES_*` rule
above) and `OTEL_{TRACES,METRICS,LOGS}_EXPORTER`.

### Availability guarantees

Telemetry must never slow a pipeline or stop the server:

- **Off:** every hook returns at once and the SDK is never imported.
- **On:**
  - Hooks never wait on anything, never touch the network, and swallow their
    own errors. Errors are counted in `rocketride.telemetry.hook_errors`.
  - Pipeline traces are built on their own thread, from a bounded queue; the
    hooks only record and queue.
  - Export runs on SDK threads with bounded queues. When the collector is down
    or slow, data is dropped, never queued without limit.
  - The SDK's own messages during a collector outage are rate-limited to one
    line per minute per message, with a count of the ones suppressed.
  - Shutdown waits at most `RR_OTEL_SHUTDOWN_TIMEOUT` seconds, then drops what is
    left.
  - Tracked state is bounded: live runs, open component calls (per run and in
    total), distinct node types and error codes, and distinct label values per
    metric. Whatever a bound drops is counted in `rocketride.telemetry.dropped`.

### Cost

Telemetry's CPU cost grows with the number of node calls traced. Measured on
one core (Python 3.12, OpenTelemetry SDK 1.45), per object through the pipeline:

| Components in the pipeline | `RR_OTEL_TRACES_SAMPLE_RATIO=0` (metrics only) | `0.1` | `1.0` |
|---|---|---|---|
| 3 | 0.07 ms | 0.12 ms | 0.29 ms |
| 50 | 1.1 ms | 1.5 ms | 5.5 ms |

The spans are built on their own thread, but they share the interpreter with
the server, so this is CPU the server process spends. For wide pipelines at a
high object rate, lower the ratio (failed and slow passes are still kept) or
`RR_OTEL_COMPONENT_SPANS`.

Measured once, on one 8-core laptop with OpenTelemetry SDK 1.45.2 (the figures
grow on a slower or busier CPU), against the same server with telemetry off:
about 25 µs per WebSocket request, 40 µs per database call (dependency
recording: `RR_OTEL_INSTRUMENT=none` removes it), 11–12 µs per component enter
or leave in the server, and 2 µs per node call in the run's process. End to end,
with four clients: a pipeline that calls an LLM (20 ms) took 2.9 ms longer per
request (2%; 7% more CPU); a pipeline doing almost no work per object, which
keeps the server's event loop saturated, the worst case, 4.4 ms longer (15%;
10% more CPU), about a third of it the component timing, the rest not yet
explained. With telemetry off the cost is nothing measurable, the engine
included. Measure it on your own hardware with the scripts in
`tools/telemetry_overhead`; its README says how each figure is computed.

## Metrics

Names and units follow the [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/).
RocketRide-specific attributes are namespaced `rocketride.*`.

No label carries a user, email, project name, task name, file name or payload. The
customer identifiers are the opaque org id (`rocketride.org.id`), which you can turn
off, and on the per-customer metrics the opaque id of the pipeline's project
(`rocketride.project.id`: the same for every run of that pipeline; at most 50 per
customer, then `_other`). A Prometheus-compatible backend shows `rocketride.task.ended` as
`rocketride_task_ended_total` and `rocketride.task.duration` as
`rocketride_task_duration_seconds`.

**From a metric to the trace.** `rocketride.turn`, `rocketride.turn.duration`,
`gen_ai.client.operation.duration`, `gen_ai.client.token.usage` and a failed
pipeline call's `http.client.request.duration` carry exemplars: the trace and span
of a measurement (the pass, the model call, the failed HTTP try). Only a kept trace
is named (a pass the sampling dropped leaves no exemplar). A failed call's pass is
kept under the `dependency_error` flood guard, so during a flood an exemplar can
name a trace that was not kept. The backend must store exemplars (Mimir:
`max_global_exemplars_per_user`).

| Metric | Type | Attributes | What it measures |
|---|---|---|---|
| `rocketride.task.started` | counter | `rocketride.task.run_kind`, `rocketride.org.id` | Runs begun |
| `rocketride.task.ended` | counter | `run_kind`, `rocketride.task.outcome` (`ok`/`error`/`cancelled`), `rocketride.task.stop_reason`, `error.type`, `rocketride.engine.error_code` (errors: the engine code the run exited with, e.g. `InvalidParam`, `Exception`; bounded), `rocketride.task.exit_signal` and `rocketride.task.oom_suspected` (a process a signal ended: `SIGKILL`, `SIGSEGV`...; suspected out of memory when it was a SIGKILL while the container's OOM kill count rose: the count is the container's, so a suspicion, not a proof), `rocketride.org.id`, `rocketride.project.id` | Runs ended. `stop_reason` is `user`, `ttl`, `disconnect`, `unregister`, `start_failed` or `none`. `error.type`, for a run that never came up, is `start_rejected` (refused with a validation code: a node that does not exist, an invalid setting, a missing or unfilled credential), `start_error` (any other error while it set up: a node's exception, a dependency, a provider) or `start_failed` (no process, killed or timed out while starting); after it came up it is `signal` (killed: OOM, crash), `exit_code` or `engine_lost` (the server died under it) |
| `rocketride.task.duration` | histogram (s) | `run_kind`, `outcome`, `rocketride.org.id`, `rocketride.project.id` | Run duration, begin to end |
| `rocketride.task.startup.duration` | histogram (s) | `run_kind` | Begin to running (the first start of a run; a restart in place is not timed) |
| `rocketride.task.count` | up-down counter | `rocketride.task.state` | Live runs by state |
| `rocketride.component.duration` | histogram (s) | `rocketride.component.type`, `rocketride.component.result` | Each node call, enter to leave. The type is the node (provider), never the id you gave the component. The result (`continue`/`skip`/`error`) is present when the run's trace level is `metadata` or higher |
| `rocketride.component.errors` | counter | `component.type`, `rocketride.component.severity`, `error.type` (errors only), `rocketride.component.code` (warnings only), `rocketride.org.id`, `rocketride.project.id` | Errors and warnings a node reports (`_engine`: reported by the engine itself). When a Python node's handler raised, the error is that node's (relayed by its process with the exception's class and where it was raised: also on its component span as an `exception` event with `code.file.path` and `code.line.number`), not guessed from the text. `error.type` is an error's code, `rocketride.component.code` a warning's (each at most 200 distinct, then `_OTHER`): the server's log keeps errors only, so a warning's code is how it is told apart. A stop's `Cancelled` messages are not counted |
| `rocketride.component.dependency_errors` | counter | `rocketride.component.type`, `error.type` (the HTTP status or exception, at most 200 distinct), `server.address` (bounded like the dependency metrics), `rocketride.org.id`, `rocketride.project.id` | Failed dependency calls a node made (relayed from the run's process): which node of which customer a provider refused, even when the node answered with an error message and went on, e.g. `401` from `api.openai.com` for a customer's API key. Failures only |
| `rocketride.object.processed` | counter | `rocketride.object.outcome` (`completed`/`failed`), `rocketride.org.id` | Objects through pipelines, as the engine counts them: an LLM node that answers with the provider's error counts as completed (see `rocketride.turn`) |
| `rocketride.turn` | counter | `rocketride.turn.outcome` (`ok`/`error`/`abandoned`), `rocketride.turn.cause` (errors: `platform`/`provider`/`customer`), `error.type` (errors: the node's kind, e.g. `provider_unavailable`, `provider_unreachable`, `provider_timeout`), `server.address` (the LLM server the pass called, bounded like the dependency metrics), `gen_ai.provider.name` and `gen_ai.request.model` (the model the pass asked: the failed call's for a failed pass, else the last; bounded as on `gen_ai.client.operation.duration`; absent when the pass asked no model), `rocketride.turn.carried` (`true`: the pass carried a question or a document through two components or more; `false`: a component's own open or close step), `rocketride.task.run_kind`, `rocketride.org.id`, `rocketride.project.id` | One per pass (a question, a document) when it ends, whatever the trace sampling: answered, failed (and whose failure: `provider` for the provider failing (5xx, unreachable, or no answer in time), `customer` for their key or quota (401, 403, 429), `platform` for anything else: a node or engine error, a request we built wrong, a pass stuck or cut off by a shutdown), or `abandoned` (open when the user stopped the run). `server.address` comes from the LLM node's or agent's relayed HTTP calls in that pass (the failing one for a failed pass; never the model server's DAP calls): it is absent when the pass made no such call or its calls were not relayed (`RR_OTEL_INSTRUMENT` without `http`), and when the failing call was left out of the pass (past its relayed-call cap) the first LLM call's server is named. A pass made two passes by a fan-out counts twice |
| `rocketride.turn.duration` | histogram (s) | `rocketride.turn.outcome` (`ok`/`error`/`abandoned`: the user stopped the run while it was open), `server.address`, `gen_ai.provider.name`, `gen_ai.request.model` (as on `rocketride.turn`), `rocketride.task.run_kind`, `rocketride.source.type` (the run's source component type: `chat`, `webhook`, `dropper`...; bounded like component types) | How long a pass (a question, a document) took, begin to end, answered or failed: the answer time a customer waits, by the LLM server it used. No org (per-customer latency is in the traces); only a pass that carried a question or a document is timed (data went through two components or more; a component's own open or close step is not), and a pass the user stopped is timed as `abandoned` (how long they waited) |
| `gen_ai.client.operation.duration` | histogram (s) | `gen_ai.operation.name`, `gen_ai.provider.name` (from the node type: `openai`, `anthropic`, `aws.bedrock`...), `gen_ai.request.model` (at most 50 per provider, then `_other`; `_unknown` when the node names none), `error.type` (failed calls) | An LLM node's model calls, tries and waits included: which provider and model fail or are slow. No org (bounded series): per customer, `rocketride.turn` names the model too |
| `gen_ai.client.token.usage` | histogram ({token}) | as above, plus `gen_ai.token.type` (`input`, cached included; `output`) | Tokens one model call used, when the provider reported them |
| `rocketride.llm.tokens` | counter | `gen_ai.token.type` (`input`/`output`, plus `cache_read`/`cache_creation`), `rocketride.org.id` | LLM tokens used by runs |
| `rocketride.ws.requests` | counter | `rocketride.ws.client` (`user`/`run_key`/`unauthenticated`), `error.type` (failed requests, as on `rpc.server.call.duration`), `rocketride.org.id` | A customer's WebSocket requests and how they failed (requests of a connection that has not signed in to an org are not counted here). No method (bounded series): the exemplar's trace names it |
| `rpc.server.call.duration` | histogram (s) | `rpc.system.name=rocketride_dap`, `rpc.method`, `error.type`, `rocketride.ws.client` | Each WebSocket request handled (sign-in and public commands included; a request refused for lack of sign-in is counted in `rocketride.auth.attempts` instead). `rpc.method` is a command with a handler, else `_OTHER`. `error.type` is the error's code when it is a code (letters, digits, `_`; at most 200 distinct), or `_OTHER` for an error without one (an exception: kept to one value on purpose, so it stays bounded and countable) |
| `rocketride.ws.connection.count` | up-down counter | | Open client connections |
| `rocketride.ws.connections` | up-down counter | `rocketride.org.id`, `rocketride.ws.client` (`user`/`run_key`/`unauthenticated`) | Open client connections per customer, by how the client signed in: `user` a person's own credential (console, IDE, SDK with an API key), `run_key` a run's `tk_`/`pk_` key (apps, webhooks, chat widgets), `unauthenticated` not signed in, or signed out (no org). Adds up to `rocketride.ws.connection.count` |
| `rocketride.ws.users` | up-down counter | `rocketride.org.id` | Distinct people connected per customer: `user` connections only (a run key carries its deploy owner, not the person using the app). Counted in the engine; user ids are never exported. Organizations past the org cap add up in `_other`: there a person in two of them counts twice (an upper bound) |
| `rocketride.ws.users.all` | up-down counter | | Distinct people connected to this engine, whatever their organization (one person in two organizations counts once). Across engines the sum is an upper bound: a person connected to two engines counts on each |
| `rocketride.ws.disconnects` | counter | `rocketride.ws.disconnect.reason` (`closed`/`error`/`unauthenticated`), `rocketride.ws.closed_by` (`client`, `server`: this server closed it, e.g. a restart; `network`: a reset or an error), `rocketride.ws.close_code` (`1000`, `1001`, `1005`, `1006`, `1008`, `1009`, `1011`, `1012`, `1013`, `_other`; `none`: no close frame), `rocketride.org.id` | Closed client connections: `closed` closed normally by either side (close code 1000, 1001 or none); `error` the connection broke (dropped without a close, 1006, e.g. a lost network; another error code; a reset); `unauthenticated` closed without ever signing in (no org). The org is the last one the connection signed in to |
| `rocketride.ws.connection.duration` | histogram (s) | `rocketride.ws.disconnect.reason`, `rocketride.ws.closed_by` | How long client connections stayed open (no org: bounded series) |
| `rocketride.ws.requests.in_flight` | up-down counter | `rpc.method` | WebSocket requests being handled (methods matching `RR_OTEL_SLOW_REQUEST_EXCLUDE` are long-lived and left out) |
| `rocketride.ws.requests.oldest` | gauge (s) | `rpc.method` | Age of the oldest request being handled: a request that never finishes keeps it growing |
| `rocketride.auth.attempts` | counter | `rocketride.auth.outcome` (`success`/`rejected`/`too_many_attempts`/`unauthenticated`), `error.type` (a rejection's code), `rocketride.org.id` | WebSocket sign-ins, and requests made before signing in (`unauthenticated`) |
| `rocketride.billing.spend_checks` | counter | `rocketride.billing.outcome`, `rocketride.org.id` | Credit checks before a pipe opens |
| `rocketride.pass.idle` | gauge (s) | `rocketride.component.type`, `rocketride.org.id` | For each innermost open node type and org, the longest a pass has gone without an enter or leave while a call is still open: a hung node or a provider that never answers. A working agent stays near 0 (its tools enter and leave); an idle source has no pass open; a run with a debugger attached (the whole run, paused or not) and the exempt types are left out. 0 once no pass is idle, then gone 15 minutes later |
| `rocketride.telemetry.up` | gauge | | 1 while telemetry exports (absence means telemetry is down) |
| `rocketride.telemetry.hook_errors` | counter | `rocketride.telemetry.hook` | Hooks that raised and were ignored |
| `rocketride.telemetry.dropped` | counter | `rocketride.telemetry.reason` | Telemetry dropped to stay within a bound: `trace_queue_full`, `span_budget`, `invocation_cap`, `run_evicted`, `stale_call` (with `RR_OTEL_STALE_CALL_SECONDS`: a call open that long was taken as a lost leave), `org_overflow`, `error_type_overflow`, `attribute_overflow`, `dependency_span_cap` (a dependency call left out of its trace), `dependency_undecided` (a dependency call whose request never ended), `dependency_metric_error`, `dependency_span_error`, `dependency_metric_overflow`, `relay_overflow` (a run's process queued more dependency calls than it holds), `pass_tid_reused` (the engine repeated a pass id: see *Dependency calls*), `info_overflow` (an info gauge, such as the organization names, past its observations per collection). By design, not a loss: `server_overflow` (a pipeline server past its customer's 20 or the 200 in all: `_other`), `pass_unwound` (a pass interrupted by an exception, exported unfinished when the next began), `error_log_duplicate`, `error_log_rate_limited`, `slow_rate_limited`, `relay_late` (a node's dependency call outside any pass), `relay_unattributed` (one made on a thread the node started), `relay_global` (a node's call from its global handler, such as a key check when the run starts: measured with the node's type, in no question's trace) |
| `db.client.operation.duration` | histogram (s) | `db.system.name` (`mysql`, `postgresql`, `redis`, …), `db.operation.name` (`SELECT`, `GET`, …, else `_OTHER`), `server.address`, `error.type` | Each database or Redis call the server makes |
| `http.client.request.duration` | histogram (s) | `http.request.method`, `server.address`, `http.response.status_code`, `error.type` | Each HTTP request the server makes (identity provider, billing, storage APIs) |
| `rpc.client.call.duration` | histogram (s) | `rpc.system.name=aws-api`, `rpc.service`, `rpc.method`, `error.type` | Each AWS API call the server makes |

The run outcome counters start at 0 for every label set they can take without an
org id, so `rate()` counts their first increment. A series that carries an org id
starts with that org's first run; on a backend that records the series' start
time (OTLP start timestamps; in Mimir,
`otel_created_timestamp_zero_ingestion_enabled`), its first increment counts too.

## Traces

- **`pipeline`**: one trace for each pass of a pipeline over an object, with a
  child `component <type>` span for each node call, and under it a CLIENT span
  for each dependency call that node made (an LLM provider's API, a database, the
  model server; see *Dependency calls*). It is exported when the pass ends, so a
  long-running deployment has live traces. The decision to keep it is made at that
  point: failed passes and slow ones are always kept; so is a pass in which a
  dependency call failed although the pass went on (`dependency_error`: a retried
  429, a 401 the node answered with an error message), under the same flood
  guard as slow ones; the rest at `RR_OTEL_TRACES_SAMPLE_RATIO`.
  The `pipeline` span carries the run (`rocketride.task.id`, and its
  `rocketride.pipeline.id` and `rocketride.source`), the pipeline's project
  (`rocketride.project.id`: opaque, the same for every run of that pipeline, also
  on the `task` span), the customer and
  `rocketride.sampling.reason`; each `component <type>` span its
  `rocketride.component.id` (as written in the pipeline). An LLM node's call to
  its model is a CLIENT span `chat <model>` (GenAI conventions:
  `gen_ai.operation.name`, `gen_ai.provider.name`, `gen_ai.request.model`,
  `gen_ai.response.model`, `gen_ai.usage.input_tokens` (cached input included),
  `gen_ai.usage.output_tokens`, `gen_ai.usage.cache_read.input_tokens`) that
  covers all its tries and the waits between them: a `rocketride.model.retry` event for each
  failed try that was retried (`error.type`, `rocketride.retry.delay_s`), and on a
  final failure an error status and an `exception` event with the provider's
  message (secrets removed). Whichever way the node asked (streaming, native
  streaming, or not), it is one model call; a stream that broke after text
  reached the user is a failed call. Its HTTP tries are its children, named
  `{method} {url.template}` (a known provider route, else `_OTHER`), with
  `server.address`, `server.port`, `http.response.status_code`, and
  `http.request.resend_count` on the second and later tries. When a node reports that its call failed
  although it went on (an LLM node answering with the provider's error), its
  component span and the `pipeline` span are marked failed with the bounded
  cause (`error.type`: `provider_auth`, `provider_rate_limited`,
  `provider_unavailable` (5xx), `provider_unreachable` (no connection: DNS, a
  refused or timed-out connect, TLS), `provider_timeout` (connected but no
  answer in time, or cut mid-answer: 504/408/499/524 or a client timeout),
  `provider_error`), and the component span has an
  `exception` event with the provider's message, secrets removed. The node's
  metrics and the run's outcome are unchanged.
- **`task`**: one trace per run, from begin to end. It carries the outcome. A run
  that ended in error always has one; the others follow
  `RR_OTEL_TRACES_SAMPLE_RATIO`. A run that ended in error says what failed: its
  status message names the kind and the run's first error (`run ended in error
  (start_error): Profile x is not defined in llm_anthropic`, up to 300 characters,
  the same redacted text as its run error log line), and an `exception` event
  carries `exception.message`, `exception.type` and the code location
  (`code.file.path`, `code.line.number`). A run killed by a signal says which
  (`rocketride.task.exit_signal`, and `rocketride.task.oom_suspected`). The span
  links to the run's kept failed and slow passes (at most 32; each link's
  `rocketride.sampling.reason`), each a `pipeline` trace of its own.
- **`<method>`** (`rpc.method`, as the RPC conventions name it): a SERVER span
  for each WebSocket request, kept when it failed, took
  `RR_OTEL_SLOW_REQUEST_SECONDS` or more, or an ERROR log line named its trace.
  When the signed-in caller sent a sampled W3C `traceparent`, the span is in the
  caller's trace and is also kept at `RR_OTEL_TRACES_SAMPLE_RATIO` (see below).
  The request's dependency calls are its children. It names who asked:
  `rocketride.org.id`, `rocketride.ws.client`, `rocketride.connection.id` (the
  connection's number in this server's log, `CONN-<n>`), and for a request about a
  live run its `rocketride.task.id` and a link to the run's `task` trace (kept
  because of the link).

### Dependency calls

The server's own calls to its database, Redis, HTTP services and AWS are traced
and measured (`RR_OTEL_INSTRUMENT`). Each call is a CLIENT span under the request
that made it, kept or dropped with that request; a call made outside any request
(a background job) is kept when it failed or was slow. Every call is measured,
kept or not (`db.client.operation.duration`, `http.client.request.duration`,
`rpc.client.call.duration`). No SQL statement or parameter, URL query or
credential leaves the server, and error messages are redacted.

The nodes' own calls are made in the run's process, which exports nothing: it
writes each call (name, times, error, server, operation or method, status, the
URL's scheme only) to the server, which measures it and places it under the node
call that made it. Nodes have no telemetry code:

- A driver a node installs when it runs (`depends()`, then its import) is
  instrumented right after the install: Qdrant's gRPC, a MongoDB client, a
  Postgres driver used without SQLAlchemy.
- Work a node hands to a thread or an executor keeps the node call it was made
  for (the threading instrumentation copies it at submit). A call that ends
  after its pass did is measured, not placed in another pass (`relay_late`).
- One call, one measurement: a client span inside another (urllib3 under
  requests or botocore) and a driver's span inside a SQLAlchemy statement are
  not measured again. A library with spans of its own that uses the global
  provider (Elasticsearch) is measured by its HTTP calls (urllib3): the run's
  process sets no global provider, so a node's own (crewai's) stays its own.
- Neo4j queries are timed to the server's first answer (the driver returns a
  lazy result); an error while the records are read is not counted on the call.
- Database calls keep their system, operation (`SELECT`, `FIND`, `MATCH`) and
  server; never the statement, the collection or the database's name. A gRPC
  call keeps its service, method, server and status (`UNAVAILABLE`,
  `DEADLINE_EXCEEDED`) as its error.
- A pipeline's servers are its customer's: at most 20 per customer and 200 in
  all, then `_other` (`server_overflow`).

The model server's latency as the pipelines see it is
`rpc.client.call.duration{rpc.system.name="rocketride_dap"}`.

While telemetry is on, the engine gives each pass a trace id (the server starts
a run's process with `--otel_pass_ids`; the id is sent as `tid` with the pass's
component enter/leave events and readable by a Python node as
`instance.passTraceId`). With telemetry off the engine sends no id and its
events are as before. A node's model server request carries the id as its
`traceparent`, so when both are kept the model server's span (queue wait, GPU
time) is a child of the node's call in the pipeline's trace. Clients never see
the `tid`: it is removed before the trace reaches them.

Each side decides on its own what it keeps, after the fact: the server keeps a
pass that failed or was slow, the model server a request that failed or was
slow. A slow inference inside a pass the server did not keep is therefore kept
by the model server alone, its parent never arriving (Tempo shows it as a trace
whose root span was not received). The same happens to a model server request made past
the per-call cap. A source that hands
one object to two pipes starts two passes, so two traces. If the engine repeated
a pass id on a pipe (its pass boundary drifted), the server gives that pass ids
of its own and counts `pass_tid_reused` once for the pass: the pipeline's traces
stay apart, but the model server's spans of that pass still carry the repeated
id. An exception between a component's enter and leave ends the level in the
engine all the same, so it does not make the next pass repeat the id; when the
next pass begins, the server exports the interrupted one unfinished
(`rocketride.span.unclosed`) and counts `pass_unwound`. A pass still open when its
run ends, the server shuts down or it is closed as stuck says by what
(`rocketride.pass.closed_by`: `task_end`, `shutdown`, ...); when a dependency call
in it had failed, its status is an error naming that failure (`pass cut by the run
ending; last model error: provider_unavailable (gpt-4o): ...`).

Database and Redis calls are frequent and fast, so the server times them itself
and builds their spans only for a request that is kept (a few tens of
microseconds a call). HTTP and AWS calls use the OpenTelemetry instrumentation
packages, which must match the SDK release. The SaaS image installs them;
elsewhere run `./engine -c "from ai.common.telemetry import _deps; _deps.install()"`
(or set `RR_OTEL_AUTO_INSTALL=true`). Without them, the server says once which
calls it does not trace and runs as before.

At the end of each run the server writes one JSON line to its output:
`{"level": "error"|"info", "event": "rocketride.task.ended", "task_id": …, "trace_id": …, "outcome": …, "org_id": …}`.
This line links the logs to the run's trace (`trace_id` is empty when the run's
trace was not sampled). `level` is `error` for a run that ended in error, so a
log pipeline that keeps errors only keeps exactly those. A pass stalled for
`RR_OTEL_STALL_LOG_SECONDS` writes one line too, with `level` `error`:
`{"event": "rocketride.pass.stalled", "task_id": …, "trace_id": …, "component_type": …, "idle_s": …, "org_id": …}`,
within a minute (it is written when metrics are collected), again if the pass
progresses and stalls once more. A WebSocket request open for
`RR_OTEL_OVERDUE_REQUEST_SECONDS` writes one:
`{"event": "rocketride.request.overdue", "method": …, "age_s": …, "trace_id": …, "task_id": …, "connection_id": …, "org_id": …}`.
The `rocketride.task.ended` and `rocketride.pass.stalled` lines also carry
`project_id`; a run a signal ended carries `exit_signal` and `oom_suspected`.

### A run's errors in the server's log

With telemetry on (or `RR_LOG_FORMAT` set), the server also writes each run's
errors to its own output, one record per error, each with `task_id`, `org_id`,
`project_id` (the pipeline: the same for each of its runs) and `trace_id`: a Python traceback from the run (as one record), a stderr line
that starts with an error level, an error a node reported, an exit with an
error (with the engine's `error_code`). Nothing else from the run's output is
written. Secrets (authorization and cookie headers, API keys and tokens, JSON web tokens,
private keys, the value of any `name=` or `"name": ` pair whose name ends in key,
token, secret or password), e-mail addresses, URL paths and
queries (a URL keeps its scheme and host) and the input a validation error
quotes (`input_value=`) are redacted; a chained exception is one record (the
exception that propagated, with its chain). Each run is rate-limited (20 records a
minute) and a repeated error is written once (both counted in
`rocketride.telemetry.dropped`); an error that names its question
(`pass_trace_id=`) is one per question. A trace named in an error record is always
kept, so the `trace_id` leads to it. The run's events (and anything reading
them, such as the Event Monitor) are unchanged.

Text (default): `ERROR rocketride.task task_id=… org_id=… project_id=… trace_id=… | message`,
with the traceback indented below. `RR_LOG_FORMAT=json`: one JSON object per
line with `ts`, `level`, `logger`, `msg`, the ids, `error_code`, `exc_type` and
`exc` (the traceback).

The run's trace id is assigned when the run begins and each pass of the
pipeline has its own, linked to the run's.

## Tracing from your application

The SDKs propagate the caller's trace context, so a request your application
makes appears inside your trace:

- **Python:** when your process uses OpenTelemetry and a span is active, every
  request carries its `traceparent` automatically (`opentelemetry-api` stays
  optional). Set `ROCKETRIDE_TRACE_PROPAGATION=false` to turn this off. Both are
  read once per process: set the variable, and install OpenTelemetry, before
  the first request.
- **TypeScript:** pass a provider. No dependency is added:

  ```ts
  import { context, propagation } from '@opentelemetry/api';

  const client = new RocketRideClient({
  	uri,
  	auth,
  	traceparent: () => {
  		const carrier: Record<string, string> = {};
  		propagation.inject(context.active(), carrier);
  		return carrier.traceparent;
  	},
  });
  ```

The server honours the caller's sampling decision: with an unsampled
`traceparent`, the request is kept only when it failed or was slow, in a trace of
its own.

## Metrics from your own code

A module running inside the server (an account extension, for instance) declares
its metrics once, at import time. Each metric is a no-op while telemetry is off
and never raises:

```python
from ai.common import telemetry

LOGINS = telemetry.Counter(
    'myorg.auth.login', '{login}', 'Sign-ins by outcome',
    attributes={'outcome': 'myorg.auth.outcome', 'org': 'rocketride.org.id'},
)

LOGINS.add(outcome='success', org=org_id)
```

A node signals a configuration the user must fix by raising `ConfigError`
(`from ai.common.config import ConfigError`, also exported by `rocketlib`): the
run is then `start_rejected`, not a start error. Use it only for problems found
without calling anything, such as an empty or unfilled credential or an invalid
setting; a provider that rejects a key or does not answer is not one. Any other
exception a node raises, a plain `ValueError` included, is a start error.

Only the declared attribute keys are exported. Values are capped in length and in
the number of distinct combinations per metric. `telemetry.Histogram` and the
`telemetry.timed(histogram, ...)` context manager measure durations; `timed`
records an exception's class as `error.type` when the histogram declares an
`error` attribute (`attributes={..., 'error': 'error.type'}`).

Pass only an org id your code resolved itself (the signed-in account's), never
one a client sent: the org ids a process attributes are capped, and a client
could otherwise fill the cap.
