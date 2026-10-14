# wc-log

WywyCode's shared logging and tracing module. Every Go repo in the bundle
(`wc-harness`, `wc-tool-call`, `wc-chat-completions`) imports it. It ships the
behaviour contract from the spec: **log to OTel, fall back to JSONL on failure,
warn the user after 5 consecutive failed sends** — plus a matching OTel tracer
provider.

Tools carry no logging shim; `wc-tool-call` logs and traces tool calls through
this module.

## Contract

- **Input only.** The library reads no environment at all. The collector
  endpoint and the log directory are required inputs on `LoggerConfig`; every
  OTel exporter knob is set explicitly. The only env that can still affect
  behavior is `OTEL_RESOURCE_ATTRIBUTES`, which the OTel SDK merges into the
  resource internally and offers no way to suppress — a documented SDK
  limitation, not a module choice.
- **Delivery.** Records go to an OTLP/HTTP collector. Delivery is synchronous,
  so the logger knows immediately whether a send succeeded.
- **Fallback.** A failed send writes the record to a local JSONL file. The
  logger `mkdir -p`s the directory before its first write. The fallback is a
  send-failure path, not a no-collector mode.
- **Warning.** After 5 consecutive failed sends the logger surfaces a warning —
  once per failure streak — to the JSONL log, to `warnings.log`, and to the
  in-memory history returned by `Warnings()` (which the harness exposes on
  `GET /health`). A later success re-arms it.
- **Paths.** `LoggerConfig.LogDir` is required and used verbatim: the logger
  writes `wc.log` and `warnings.log` there. It never consults `WC_LOG_DIR`,
  `XDG_STATE_HOME`, or `HOME` — the caller decides the location (e.g. resolve
  `$XDG_STATE_HOME/wc/logs` itself) and passes it in.
- **Tracing.** `Tracer(name)` returns a real OTLP tracer when a collector is
  configured, and a no-op tracer otherwise.

## Usage

```go
import (
	"go.opentelemetry.io/otel/attribute"
	wclog "github.com/WywySenarios/wc-log/log"
)

logger, err := wclog.New(wclog.LoggerConfig{
	ServiceName: "wc-harness",
	Endpoint:    "http://otel-collector:4318",
	LogDir:      "/home/wc/.local/state/wc/logs",
})
if err != nil {
	return err
}
defer logger.Shutdown()

logger.Info("turn started", attribute.String("session_id", id))

tracer := logger.Tracer("wc-tool-call")
ctx, span := tracer.Start(ctx, "tool.exec")
defer span.End()
```

Both `Endpoint` and `LogDir` are **required**. `Endpoint` must be an `http(s)`
URL — the scheme decides TLS (`http` = plaintext, `https` = TLS), and a trailing
slash is ignored. `New` returns an error if either is missing or malformed,
rather than silently degrading or guessing a location. The caller owns all
configuration; the library reads no environment and falls back to nothing.

## Consuming

`wc-log` is a tagged Go module consumed by module path — it is fetched via
GOPROXY at a published tag, never through a submodule. Consumers may use a local
`replace` during development, but a local-path `replace` must never reach CI.
Run the canonical gate in every consumer:

```sh
scripts/check-no-local-replace            # checks ./go.mod
```

## Development

```sh
go test ./...
```

All suites run from `scripts/tests/` and are also wired into pre-commit and CI.
