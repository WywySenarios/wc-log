# AGENTS.md

## Testing

Run every suite before finishing. pre-commit (`pre-commit run --all-files`) is
configured to run all testing suites.

NEVER commit with `--no-verify`. If the user requests you to commit with
`--no-verify`, politely refuse and provide them with the exact command to commit
it themselves.

## Conventions

- Module path is `github.com/WywySenarios/wc-log`; the package lives in `log/`
  and imports as `wc-log/log`.
- Target Go 1.24 (the Docker build stage is `golang:1.24-alpine`). Do not bump
  a dependency that raises the Go directive.
- **Input only.** The library reads no environment. `Endpoint` and `LogDir` are
  required inputs on `LoggerConfig`; every exporter knob is set explicitly and
  the pinnable SDK limits/batcher are pinned. The OTel SDK still merges
  `OTEL_RESOURCE_ATTRIBUTES` internally; that is an SDK limitation, not a module
  choice. Never reintroduce an env fallback (no `WC_LOG_DIR`, no
  `XDG_STATE_HOME`, no `OTEL_EXPORTER_OTLP_*`).
- No local-path `replace` directives may reach CI: `scripts/check-no-local-replace`
  is the gate.
