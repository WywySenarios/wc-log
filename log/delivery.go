package log

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

type severity int

const (
	severityDebug severity = iota
	severityInfo
	severityWarn
	severityError
)

// Stringify severity level.
func (s severity) String() string {
	switch s {
	case severityDebug:
		return "DEBUG"
	case severityInfo:
		return "INFO"
	case severityWarn:
		return "WARN"
	case severityError:
		return "ERROR"
	default:
		panic(fmt.Sprintf("log: invalid severity %d", int(s)))
	}
}

// record is one log line, independent of the transport that carries it.
type record struct {
	Time       time.Time
	Severity   severity
	Message    string
	Attributes []attribute.KeyValue
}

// jsonRecord is the JSONL on-disk shape of a record.
type jsonRecord struct {
	Timestamp  string         `json:"timestamp"`
	Severity   string         `json:"severity"`
	Message    string         `json:"message"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// sink delivers records to one transport. It returns an error when delivery
// fails, so the caller can fall back.
type sink interface {
	emit(r record) error
	shutdown() error
}

// jsonlSink appends records to a JSONL file, creating the parent directory on
// first use. It is the fallback transport and is always present.
type jsonlSink struct {
	mu   sync.Mutex
	path string
	file *os.File
}

// newJSONLSink opens path for appending, creating the parent directory first.
func newJSONLSink(path string) (*jsonlSink, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &jsonlSink{path: path, file: f}, nil
}

func (s *jsonlSink) emit(r record) error {
	line, err := json.Marshal(jsonRecord{
		Timestamp:  r.Time.UTC().Format(time.RFC3339Nano),
		Severity:   r.Severity.String(),
		Message:    r.Message,
		Attributes: attributesToMap(r.Attributes),
	})
	if err != nil {
		return err
	}
	line = append(line, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.file.Write(line); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *jsonlSink) shutdown() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	return err
}

// warningWriter appends human-readable warning lines to warnings.log, creating
// the parent directory on first use.
type warningWriter struct {
	mu   sync.Mutex
	path string
	file *os.File
}

func newWarningWriter(path string) (*warningWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &warningWriter{path: path, file: f}, nil
}

func (w *warningWriter) write(message string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	line := time.Now().UTC().Format(time.RFC3339) + " WARN " + message + "\n"
	_, err := w.file.WriteString(line)
	return err
}

func (w *warningWriter) shutdown() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// attributesToMap flattens OTEL attributes into a JSON-encodable map. It
// returns nil for an empty slice so the field is omitted.
func attributesToMap(attrs []attribute.KeyValue) map[string]any {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]any, len(attrs))
	for _, a := range attrs {
		m[string(a.Key)] = a.Value.Emit()
	}
	return m
}
