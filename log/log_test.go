package log

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

// unreachableEndpoint is a syntactically valid endpoint that refuses
// connections instantly, so tests never touch a real collector.
const unreachableEndpoint = "http://127.0.0.1:1"

// fakeSink is a controllable primary transport for exercising fallback and the
// consecutive-failure counter without a network.
type fakeSink struct {
	mu      sync.Mutex
	err     error
	failFor int // >0: next failFor emits fail; 0: never; <0: always
	emitted []record
	downs   int
}

func (f *fakeSink) emit(r record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.emitted = append(f.emitted, r)
	switch {
	case f.failFor > 0:
		f.failFor--
		return f.err
	case f.failFor < 0:
		return f.err
	default:
		return nil
	}
}

func (f *fakeSink) shutdown() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.downs++
	return nil
}

func (f *fakeSink) setFailure(failFor int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failFor = failFor
}

func (f *fakeSink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.emitted)
}

// newTestLogger assembles a Logger with a fake primary sink and a temp-dir
// JSONL fallback so tests never touch the real state directory.
func newTestLogger(t *testing.T, primary sink) (*Logger, string, string) {
	t.Helper()
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wc.log")
	warnPath := filepath.Join(dir, "warnings.log")

	fallback, err := newJSONLSink(logPath)
	if err != nil {
		t.Fatalf("newJSONLSink: %v", err)
	}
	warnings, err := newWarningWriter(warnPath)
	if err != nil {
		t.Fatalf("newWarningWriter: %v", err)
	}
	l := &Logger{primary: primary, fallback: fallback, warningsFile: warnings}
	t.Cleanup(func() { _ = l.Shutdown() })
	return l, logPath, warnPath
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read %s: %v", path, err)
	}
	trimmed := strings.TrimRight(string(data), "\n")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "\n")
}

// TestOtelAvailablePath proves a successful primary send is the only delivery:
// nothing lands in the JSONL fallback and no warning is raised.
func TestOtelAvailablePath(t *testing.T) {
	primary := &fakeSink{}
	l, logPath, _ := newTestLogger(t, primary)

	l.Info("hello", attribute.String("k", "v"))

	if got := primary.count(); got != 1 {
		t.Fatalf("primary receives = %d, want 1", got)
	}
	if lines := readLines(t, logPath); len(lines) != 0 {
		t.Fatalf("fallback lines = %d, want 0: %v", len(lines), lines)
	}
	if got := len(l.Warnings()); got != 0 {
		t.Fatalf("warnings = %d, want 0", got)
	}
}

// TestOtelUnreachableWritesJSONL proves a failed primary send falls back to the
// JSONL file.
func TestOtelUnreachableWritesJSONL(t *testing.T) {
	primary := &fakeSink{err: os.ErrDeadlineExceeded, failFor: -1}
	l, logPath, _ := newTestLogger(t, primary)

	l.Info("first", attribute.String("a", "1"))
	l.Warn("second", attribute.Int("n", 2))

	lines := readLines(t, logPath)
	if len(lines) != 2 {
		t.Fatalf("fallback lines = %d, want 2: %v", len(lines), lines)
	}

	var firstRecord jsonRecord
	if err := json.Unmarshal([]byte(lines[0]), &firstRecord); err != nil {
		t.Fatalf("unmarshal fallback line: %v", err)
	}
	if firstRecord.Severity != "INFO" || firstRecord.Message != "first" {
		t.Fatalf("first record = %+v", firstRecord)
	}
	if firstRecord.Attributes["a"] != "1" {
		t.Fatalf("first record attributes = %+v", firstRecord.Attributes)
	}
	if firstRecord.Timestamp == "" {
		t.Fatal("timestamp is empty")
	}
}

// TestWarningFiresExactlyOnceAtThreshold proves the warning fires once when the
// fifth consecutive send fails and does not repeat while the streak continues.
func TestWarningFiresExactlyOnceAtThreshold(t *testing.T) {
	primary := &fakeSink{err: os.ErrDeadlineExceeded, failFor: -1}
	l, logPath, warnPath := newTestLogger(t, primary)

	for i := 0; i < 7; i++ {
		l.Info("boom")
	}

	if got := len(l.Warnings()); got != 1 {
		t.Fatalf("warnings = %d, want 1", got)
	}
	// 7 failed INFO records + 1 warning record.
	if lines := readLines(t, logPath); len(lines) != 8 {
		t.Fatalf("fallback lines = %d, want 8: %v", len(lines), lines)
	}
	if lines := readLines(t, warnPath); len(lines) != 1 {
		t.Fatalf("warnings.log lines = %d, want 1: %v", len(lines), lines)
	}
}

// TestWarningRearmsAfterRecovery proves a success resets the streak, so a later
// streak of failures warns again.
func TestWarningRearmsAfterRecovery(t *testing.T) {
	primary := &fakeSink{err: os.ErrDeadlineExceeded, failFor: 5}
	l, _, _ := newTestLogger(t, primary)

	for i := 0; i < 5; i++ {
		l.Info("boom")
	}
	if got := len(l.Warnings()); got != 1 {
		t.Fatalf("warnings after first streak = %d, want 1", got)
	}

	// Recovery: the sixth emit succeeds.
	l.Info("ok")

	primary.setFailure(5)
	for i := 0; i < 5; i++ {
		l.Info("boom again")
	}
	if got := len(l.Warnings()); got != 2 {
		t.Fatalf("warnings after second streak = %d, want 2", got)
	}
}

// TestNewErrorsWithoutEndpoint proves an empty endpoint is rejected outright:
// the JSONL file is a send-failure fallback, not a no-collector mode.
func TestNewErrorsWithoutEndpoint(t *testing.T) {
	if _, err := New(LoggerConfig{ServiceName: "test", LogDir: t.TempDir()}); err == nil {
		t.Fatal("New succeeded with no OTLP endpoint; want an error")
	}
}

// TestNewErrorsWithoutLogDir proves the log directory is required input.
func TestNewErrorsWithoutLogDir(t *testing.T) {
	if _, err := New(LoggerConfig{ServiceName: "test", Endpoint: unreachableEndpoint}); err == nil {
		t.Fatal("New succeeded with no log directory; want an error")
	}
}

// TestNewIgnoresEnvironmentEndpoint proves the library reads no environment: a
// perfectly valid OTEL_EXPORTER_OTLP_ENDPOINT does not satisfy the required
// input.
func TestNewIgnoresEnvironmentEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:9999")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://127.0.0.1:9999")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://127.0.0.1:9999")

	if _, err := New(LoggerConfig{ServiceName: "test", LogDir: t.TempDir()}); err == nil {
		t.Fatal("New accepted an endpoint from the environment; want an error")
	}
}

// TestLogDirUsedVerbatim proves the log directory is exactly what the caller
// passed — the library never consults WC_LOG_DIR or XDG_STATE_HOME.
func TestLogDirUsedVerbatim(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("WC_LOG_DIR", t.TempDir())     // must be ignored
	t.Setenv("XDG_STATE_HOME", t.TempDir()) // must be ignored

	l, err := New(LoggerConfig{ServiceName: "test", Endpoint: unreachableEndpoint, LogDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = l.Shutdown() }()

	if got, want := l.LogPath(), filepath.Join(dir, "wc.log"); got != want {
		t.Fatalf("LogPath = %q, want %q", got, want)
	}
	if got, want := l.WarningsPath(), filepath.Join(dir, "warnings.log"); got != want {
		t.Fatalf("WarningsPath = %q, want %q", got, want)
	}
	l.Info("here")
	if lines := readLines(t, filepath.Join(dir, "wc.log")); len(lines) != 1 {
		t.Fatalf("fallback lines = %d, want 1", len(lines))
	}
}

// TestNewCreatesMissingParentDir proves the logger mkdir -p's its directory.
func TestNewCreatesMissingParentDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "does", "not", "exist")

	l, err := New(LoggerConfig{ServiceName: "test", Endpoint: unreachableEndpoint, LogDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = l.Shutdown() }()

	l.Info("created")
	if lines := readLines(t, filepath.Join(dir, "wc.log")); len(lines) != 1 {
		t.Fatalf("fallback lines = %d, want 1", len(lines))
	}
	if _, err := os.Stat(filepath.Join(dir, "warnings.log")); err != nil {
		t.Fatalf("warnings.log not created: %v", err)
	}
}

// TestNewWithEndpointBuildsOtel proves an endpoint wires up the OTLP-HTTP sink
// and a real tracer provider without opening a connection.
func TestNewWithEndpointBuildsOtel(t *testing.T) {
	l, err := New(LoggerConfig{ServiceName: "test", Endpoint: "http://127.0.0.1:4318", LogDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = l.Shutdown() }()

	if l.primary == nil {
		t.Fatal("primary is nil with an endpoint configured")
	}
	if l.tracer == nil {
		t.Fatal("tracer provider is nil with an endpoint configured")
	}
	if l.Tracer("x") == nil {
		t.Fatal("Tracer is nil")
	}
}

func TestParseEndpoint(t *testing.T) {
	valid := []struct {
		in   string
		path string
	}{
		{"http://collector:4318", ""},
		{"https://collector:4318", ""},
		{"http://collector:4318/v1/logs", "/v1/logs"},
		{"https://host.example/otlp", "/otlp"},
		{"http://collector:4318/", ""},
		{"http://collector:4318/v1/logs/", "/v1/logs"},
	}
	for _, tc := range valid {
		ep, err := parseEndpoint(tc.in)
		if err != nil {
			t.Errorf("parseEndpoint(%q) error: %v", tc.in, err)
			continue
		}
		if ep.url != tc.in || ep.path != tc.path {
			t.Errorf("parseEndpoint(%q) = %+v, want url=%q path=%q", tc.in, ep, tc.in, tc.path)
		}
	}

	invalid := []string{
		"",                     // missing
		"collector:4318",       // no scheme
		"ftp://collector:4318", // wrong scheme
		"http:///v1/logs",      // no host
		"://collector:4318",    // unparseable
		"http://",              // no host
	}
	for _, in := range invalid {
		if _, err := parseEndpoint(in); err == nil {
			t.Errorf("parseEndpoint(%q) = nil error, want error", in)
		}
	}
}

func TestSeverityString(t *testing.T) {
	cases := map[severity]string{
		severityDebug: "DEBUG",
		severityInfo:  "INFO",
		severityWarn:  "WARN",
		severityError: "ERROR",
	}
	for sev, want := range cases {
		if got := sev.String(); got != want {
			t.Errorf("severity(%d) = %q, want %q", sev, got, want)
		}
	}

	t.Run("panics on invalid severity", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("severity(99).String() did not panic")
			}
		}()
		_ = severity(99).String()
	})
}

// TestExporterPostsToSignalPath drives the real OTLP/HTTP exporter and proves
// requests land on the signal path — including for a trailing-slash endpoint,
// which must not be treated as an explicit "/" path.
func TestExporterPostsToSignalPath(t *testing.T) {
	for _, suffix := range []string{"", "/"} {
		t.Run("endpoint suffix "+suffix, func(t *testing.T) {
			paths := make(chan string, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case paths <- r.URL.Path:
				default:
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			l, err := New(LoggerConfig{ServiceName: "test", Endpoint: srv.URL + suffix, LogDir: t.TempDir()})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer func() { _ = l.Shutdown() }()

			l.Info("hi")

			select {
			case got := <-paths:
				if got != "/v1/logs" {
					t.Fatalf("request path = %q, want /v1/logs", got)
				}
			default:
				t.Fatal("collector received no request")
			}
		})
	}
}

// TestExporterNormalizesCustomPath proves a custom path with a trailing slash
// is trimmed before export.
func TestExporterNormalizesCustomPath(t *testing.T) {
	paths := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case paths <- r.URL.Path:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	l, err := New(LoggerConfig{ServiceName: "test", Endpoint: srv.URL + "/custom/", LogDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = l.Shutdown() }()

	l.Info("hi")

	select {
	case got := <-paths:
		if got != "/custom" {
			t.Fatalf("request path = %q, want /custom", got)
		}
	default:
		t.Fatal("collector received no request")
	}
}

// TestExporterIgnoresEnvironmentHeaders proves OTEL_EXPORTER_OTLP_HEADERS is
// not applied — the library reads no OTel environment.
func TestExporterIgnoresEnvironmentHeaders(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-leak=yes")

	seen := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case seen <- r.Header.Get("x-leak"):
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	l, err := New(LoggerConfig{ServiceName: "test", Endpoint: srv.URL, LogDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = l.Shutdown() }()

	l.Info("hi")

	select {
	case got := <-seen:
		if got != "" {
			t.Fatalf("collector saw leaked header x-leak=%q", got)
		}
	default:
		t.Fatal("collector received no request")
	}
}

// TestConcurrentEmit exercises the concurrency claim under -race.
func TestConcurrentEmit(t *testing.T) {
	primary := &fakeSink{}
	l, _, _ := newTestLogger(t, primary)

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l.Info("x")
		}()
	}
	wg.Wait()

	if got := primary.count(); got != n {
		t.Fatalf("primary receives = %d, want %d", got, n)
	}
}

// TestShutdownIdempotent proves Shutdown can be called twice and that emitting
// after shutdown is a no-op.
func TestShutdownIdempotent(t *testing.T) {
	primary := &fakeSink{}
	l, _, _ := newTestLogger(t, primary)

	if err := l.Shutdown(); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if err := l.Shutdown(); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	l.Info("after shutdown")
	if got := primary.count(); got != 0 {
		t.Fatalf("primary receives after shutdown = %d, want 0", got)
	}
}

// TestWarningsNotBlockedBySlowSend proves Warnings() (the GET /health source)
// does not stall behind an in-flight network send.
func TestWarningsNotBlockedBySlowSend(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	l, err := New(LoggerConfig{ServiceName: "test", Endpoint: srv.URL, LogDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = l.Shutdown() }()

	sendDone := make(chan struct{})
	go func() {
		l.Info("slow")
		close(sendDone)
	}()

	warningsDone := make(chan struct{})
	go func() {
		_ = l.Warnings()
		close(warningsDone)
	}()

	select {
	case <-warningsDone:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("Warnings() blocked behind an in-flight send")
	}

	close(release)
	<-sendDone
}
