# Contributing to wc-log

## Layout

- `log/` — the module's only package (`wc/log`).
- `scripts/check-no-local-replace` — the consumer build-hygiene gate.
- `scripts/tests/` — test suites.
- `.github/workflows/ci.yml` — CI.

## Testing

`scripts/tests/` suites must pass before commit. pre-commit runs them.

```sh
pre-commit run
```

It is highly advised to run tests before making modifications to set a baseline.

## Consuming contract

`wc-log` is fetched via GOPROXY at a published tag. Consumers must not build
against a local checkout in CI; `scripts/check-no-local-replace` enforces that.
