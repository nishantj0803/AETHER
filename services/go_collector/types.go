package main

import (
	"encoding/json"
	"time"
)

// LogRecord represents a structured telemetry event consumed from Kafka/Redpanda.
type LogRecord struct {
	EventID     string                 `json:"event_id"`
	Timestamp   string                 `json:"timestamp"`
	ServiceName string                 `json:"service_name"`
	Level       string                 `json:"level"`
	Message     string                 `json:"message"`
	TraceID     *string                `json:"trace_id,omitempty"`
	SpanID      *string                `json:"span_id,omitempty"`
	HTTPStatus  *int                   `json:"http_status,omitempty"`
	DurationMS  *float64               `json:"duration_ms,omitempty"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
	Embedding   []float32              `json:"embedding,omitempty"`
}

// ParsedTimestamp converts the ISO-8601 string to time.Time, defaulting to time.Now().
func (r *LogRecord) ParsedTimestamp() time.Time {
	if r.Timestamp == "" {
		return time.Now().UTC()
	}
	t, err := time.Parse(time.RFC3339, r.Timestamp)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, r.Timestamp)
		if err != nil {
			return time.Now().UTC()
		}
	}
	return t.UTC()
}

// MetadataJSON serializes the metadata map to a JSON string for PostgreSQL JSONB storage.
func (r *LogRecord) MetadataJSON() []byte {
	if r.Metadata == nil {
		return []byte("{}")
	}
	b, err := json.Marshal(r.Metadata)
	if err != nil {
		return []byte("{}")
	}
	return b
}

// Config encapsulates environment configuration for the Go collector.
type Config struct {
	KafkaBrokers    []string
	TopicLogs       string
	TopicDLQ        string
	ConsumerGroup   string
	DatabaseURL     string
	WorkerCount     int
	BatchSize       int
	BatchTimeoutMS  int
	DedupCapacity   int
	PrometheusPort  string
}
