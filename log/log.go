// Package log is WywyCode's shared logging and tracing module.
// It delivers records to an OTLP/HTTP collector.
// It falls back to a local JSONL file when a send fails, and surfaces a warning after consecutive failures.
package log

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// The number of consecutive failed OTLP sends after which a
// warning is surfaced. The warning fires once per failure streak.
const failureThreshold = 5

// In-memory warning history cap
const recentWarningLimit = 32

// Logger configuration.
type LoggerConfig struct {
	// ServiceName is reported to the collector as the OTel resource's
	// service.name.
	ServiceName string
	// The target OTLP/HTTP endpoint, e.g. "http://collector:4318". Required;
	// must be an http(s) URL. The library reads no environment, so this is the
	// only endpoint source and New errors when it is missing or malformed. The
	// scheme decides TLS (http = plaintext, https = TLS).
	Endpoint string
	// Headers are additional OTLP request headers.
	Headers map[string]string
	// LogDir is the directory holding wc.log and warnings.log. Required; the
	// library never derives it from the environment. The caller decides the
	// location (e.g. $XDG_STATE_HOME/wc/logs) and passes it in.
	LogDir string
}

// Warning is a surfaced delivery-failure warning.
type Warning struct {
	Time    time.Time
	Message string
}

// Logger is the shared logger. It is safe for concurrent use.
type Logger struct {
	mu           sync.Mutex
	primary      sink
	fallback     *jsonlSink
	warningsFile *warningWriter
	tracer       *sdktrace.TracerProvider // nil => noop tracer
	consecutive  int
	warned       bool
	recent       []Warning
	closed       bool
}

// New builds a Logger.
func New(cfg LoggerConfig) (*Logger, error) {
	ep, err := parseEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	if cfg.LogDir == "" {
		return nil, errors.New("log: no log directory configured; set LoggerConfig.LogDir")
	}

	fallback, err := newJSONLSink(filepath.Join(cfg.LogDir, "wc.log"))
	if err != nil {
		return nil, err
	}
	warnings, err := newWarningWriter(filepath.Join(cfg.LogDir, "warnings.log"))
	if err != nil {
		_ = fallback.shutdown()
		return nil, err
	}

	primary, tracer, err := newOtel(ep, cfg)
	if err != nil {
		_ = fallback.shutdown()
		_ = warnings.shutdown()
		return nil, err
	}

	return &Logger{primary: primary, fallback: fallback, warningsFile: warnings, tracer: tracer}, nil
}

// Debug logs at DEBUG severity.
func (l *Logger) Debug(message string, attrs ...attribute.KeyValue) {
	l.emit(record{Severity: severityDebug, Message: message, Attributes: attrs})
}

// Info logs at INFO severity.
func (l *Logger) Info(message string, attrs ...attribute.KeyValue) {
	l.emit(record{Severity: severityInfo, Message: message, Attributes: attrs})
}

// Warn logs at WARN severity.
func (l *Logger) Warn(message string, attrs ...attribute.KeyValue) {
	l.emit(record{Severity: severityWarn, Message: message, Attributes: attrs})
}

// Error logs at ERROR severity.
func (l *Logger) Error(message string, attrs ...attribute.KeyValue) {
	l.emit(record{Severity: severityError, Message: message, Attributes: attrs})
}

// Tracer returns a tracer named name. It is a no-op tracer when no OTLP
// endpoint is configured.
func (l *Logger) Tracer(name string) trace.Tracer {
	if l.tracer == nil {
		return noop.NewTracerProvider().Tracer(name)
	}
	return l.tracer.Tracer(name)
}

// Warnings returns the most recently surfaced warnings, oldest first.
func (l *Logger) Warnings() []Warning {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Warning, len(l.recent))
	copy(out, l.recent)
	return out
}

// LogPath is the JSONL fallback file's path.
func (l *Logger) LogPath() string { return l.fallback.path }

// WarningsPath is the warnings file's path.
func (l *Logger) WarningsPath() string { return l.warningsFile.path }

// Shutdown flushes and closes every transport. It is idempotent.
func (l *Logger) Shutdown() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true

	var firstErr error
	record := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if l.primary != nil {
		record(l.primary.shutdown())
	}
	if l.tracer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		record(l.tracer.Shutdown(ctx))
		cancel()
	}
	record(l.fallback.shutdown())
	record(l.warningsFile.shutdown())
	return firstErr
}

// emit delivers one record. A failed primary send increments the consecutive
// failure count, writes the record to the fallback, and — exactly once when the
// threshold is crossed — surfaces a warning.
//
// The send runs outside l.mu so a slow or retrying collector cannot block
// Warnings() (the GET /health source) or other log calls.
func (l *Logger) emit(r record) {
	if r.Time.IsZero() {
		r.Time = time.Now()
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	primary := l.primary
	l.mu.Unlock()

	err := primary.emit(r)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}

	if err == nil {
		l.consecutive = 0
		l.warned = false
		return
	}

	l.consecutive++
	_ = l.fallback.emit(r)
	if l.consecutive >= failureThreshold && !l.warned {
		l.warned = true
		l.raiseWarningLocked()
	}
}

// raiseWarningLocked surfaces the threshold warning to the fallback log, the
// warnings file, and the in-memory history. The caller holds l.mu.
func (l *Logger) raiseWarningLocked() {
	message := fmt.Sprintf(
		"log delivery to OTLP failed %d times in a row; records are being written to the JSONL fallback",
		failureThreshold,
	)
	now := time.Now()

	_ = l.fallback.emit(record{Time: now, Severity: severityWarn, Message: message})
	_ = l.warningsFile.write(message)

	l.recent = append(l.recent, Warning{Time: now, Message: message})
	if len(l.recent) > recentWarningLimit {
		l.recent = l.recent[len(l.recent)-recentWarningLimit:]
	}
}
