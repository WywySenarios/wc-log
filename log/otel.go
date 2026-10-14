package log

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otlptrace "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// otelSink delivers records to an OTLP collector over HTTP. Delivery is
// synchronous (a simple processor), so emit observes the export result and the
// logger can fall back the instant a send fails.
type otelSink struct {
	sendMu   sync.Mutex // serializes sends so lastErr is unambiguous
	errMu    sync.Mutex // guards lastErr
	lastErr  error
	provider *sdklog.LoggerProvider
	logger   otellog.Logger
}

// captureExporter records the result of each export so otelSink.emit can return
// it. The OTel SDK's simple processor swallows the exporter error into
// otel.Handle, which is not usable for per-send fallback.
type captureExporter struct {
	inner sdklog.Exporter
	sink  *otelSink
}

func (c *captureExporter) Export(ctx context.Context, records []sdklog.Record) error {
	err := c.inner.Export(ctx, records)
	c.sink.errMu.Lock()
	c.sink.lastErr = err
	c.sink.errMu.Unlock()
	return err
}

func (c *captureExporter) Shutdown(ctx context.Context) error {
	return c.inner.Shutdown(ctx)
}

func (c *captureExporter) ForceFlush(ctx context.Context) error {
	return c.inner.ForceFlush(ctx)
}

func (s *otelSink) emit(r record) error {
	// Serialize sends: Logger.emit runs outside its own lock (so Warnings()
	// never blocks on a slow collector), and concurrent sends must not clobber
	// the shared lastErr.
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	s.errMu.Lock()
	s.lastErr = nil
	s.errMu.Unlock()

	var rec otellog.Record
	rec.SetTimestamp(r.Time)
	rec.SetObservedTimestamp(time.Now())
	rec.SetSeverity(toOtelSeverity(r.Severity))
	rec.SetSeverityText(r.Severity.String())
	rec.SetBody(otellog.StringValue(r.Message))
	if len(r.Attributes) > 0 {
		kvs := make([]otellog.KeyValue, 0, len(r.Attributes))
		for _, a := range r.Attributes {
			kvs = append(kvs, otellog.KeyValueFromAttribute(a))
		}
		rec.AddAttributes(kvs...)
	}

	s.logger.Emit(context.Background(), rec)

	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.lastErr
}

func (s *otelSink) shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.provider.Shutdown(ctx)
}

// defaultExportTimeout pins the OTLP export timeout so the exporter never
// consults OTEL_EXPORTER_OTLP_TIMEOUT.
const defaultExportTimeout = 10 * time.Second

// Pinned SDK limits, matching the SDK defaults, so OTEL_LOGRECORD_* and
// OTEL_SPAN_* cannot change behavior.
const (
	attributeCountLimit       = 128
	attributeValueLengthLimit = -1
)

// defaultSpanLimits returns the SDK's default span limits, built explicitly
// rather than via NewSpanLimits so OTEL_SPAN_* is not consulted.
func defaultSpanLimits() sdktrace.SpanLimits {
	return sdktrace.SpanLimits{
		AttributeValueLengthLimit:   sdktrace.DefaultAttributeValueLengthLimit,
		AttributeCountLimit:         sdktrace.DefaultAttributeCountLimit,
		EventCountLimit:             sdktrace.DefaultEventCountLimit,
		LinkCountLimit:              sdktrace.DefaultLinkCountLimit,
		AttributePerEventCountLimit: sdktrace.DefaultAttributePerEventCountLimit,
		AttributePerLinkCountLimit:  sdktrace.DefaultAttributePerLinkCountLimit,
	}
}

// endpoint is a validated OTLP endpoint.
type endpoint struct {
	url  string // the endpoint exactly as given, scheme included
	path string // URL path with trailing slashes trimmed; empty means the signal default
}

// parseEndpoint validates raw and rejects anything that is not an http(s) URL.
// The library takes the endpoint as input only — it never reads the OTEL
// environment — so an invalid value fails loudly instead of silently falling
// back to an exporter default.
func parseEndpoint(raw string) (endpoint, error) {
	if raw == "" {
		return endpoint{}, errors.New("log: no OTLP endpoint configured; set LoggerConfig.Endpoint")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return endpoint{}, fmt.Errorf("log: invalid OTLP endpoint %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return endpoint{}, fmt.Errorf("log: invalid OTLP endpoint %q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return endpoint{}, fmt.Errorf("log: invalid OTLP endpoint %q: missing host", raw)
	}
	return endpoint{url: raw, path: strings.TrimRight(u.Path, "/")}, nil
}

// newOtel builds the OTLP-HTTP log sink and a matching tracer provider. Every
// exporter knob is set explicitly — endpoint, path, TLS, headers, compression,
// timeout — so no OTEL_EXPORTER_OTLP_* environment variable can change behavior.
func newOtel(ep endpoint, cfg LoggerConfig) (*otelSink, *sdktrace.TracerProvider, error) {
	res := resource.NewSchemaless(attribute.String("service.name", cfg.ServiceName))

	logExporter, err := otlploghttp.New(context.Background(), logExporterOptions(ep, cfg)...)
	if err != nil {
		return nil, nil, err
	}

	s := &otelSink{}
	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(&captureExporter{inner: logExporter, sink: s})),
		sdklog.WithResource(res),
		sdklog.WithAttributeCountLimit(attributeCountLimit),
		sdklog.WithAttributeValueLengthLimit(attributeValueLengthLimit),
	)
	s.provider = provider
	s.logger = provider.Logger("wc")

	traceExporter, err := otlptrace.New(context.Background(), traceExporterOptions(ep, cfg)...)
	if err != nil {
		_ = provider.Shutdown(context.Background())
		return nil, nil, err
	}
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter,
			sdktrace.WithMaxQueueSize(sdktrace.DefaultMaxQueueSize),
			sdktrace.WithMaxExportBatchSize(sdktrace.DefaultMaxExportBatchSize),
			sdktrace.WithBatchTimeout(sdktrace.DefaultScheduleDelay*time.Millisecond),
			sdktrace.WithExportTimeout(sdktrace.DefaultExportTimeout*time.Millisecond),
		),
		sdktrace.WithResource(res),
		sdktrace.WithRawSpanLimits(defaultSpanLimits()),
	)

	return s, tracerProvider, nil
}

// logExporterOptions sets every knob the log exporter reads from the
// environment. WithEndpointURL pins the endpoint and TLS scheme; WithURLPath
// then forces the normalized path (the signal default when the caller gave
// none), because the SDK otherwise re-reads the raw URL's path — trailing slash
// included.
func logExporterOptions(ep endpoint, cfg LoggerConfig) []otlploghttp.Option {
	return []otlploghttp.Option{
		otlploghttp.WithEndpointURL(ep.url),
		otlploghttp.WithURLPath(orDefault(ep.path, "/v1/logs")),
		otlploghttp.WithHeaders(cfg.Headers),
		otlploghttp.WithCompression(otlploghttp.NoCompression),
		otlploghttp.WithTimeout(defaultExportTimeout),
		otlploghttp.WithTLSClientConfig(nil),
	}
}

func traceExporterOptions(ep endpoint, cfg LoggerConfig) []otlptrace.Option {
	return []otlptrace.Option{
		otlptrace.WithEndpointURL(ep.url),
		otlptrace.WithURLPath(orDefault(ep.path, "/v1/traces")),
		otlptrace.WithHeaders(cfg.Headers),
		otlptrace.WithCompression(otlptrace.NoCompression),
		otlptrace.WithTimeout(defaultExportTimeout),
		otlptrace.WithTLSClientConfig(nil),
	}
}

// orDefault returns value, or fallback when value is empty.
func orDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func toOtelSeverity(s severity) otellog.Severity {
	switch s {
	case severityDebug:
		return otellog.SeverityDebug
	case severityInfo:
		return otellog.SeverityInfo
	case severityWarn:
		return otellog.SeverityWarn
	default:
		return otellog.SeverityError
	}
}
