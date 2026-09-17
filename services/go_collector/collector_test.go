package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

// TestLRUDeduplicationCache verifies hit, miss, and capacity-based eviction invariants.
func TestLRUDeduplicationCache(t *testing.T) {
	cache := NewLRUDeduplicationCache(3)

	// Invariant 1: Unseen keys return false
	if cache.Has("event-1") {
		t.Errorf("Expected event-1 to be absent initially")
	}

	// Invariant 2: Added keys return true
	cache.Add("event-1")
	cache.Add("event-2")
	cache.Add("event-3")

	if !cache.Has("event-1") || !cache.Has("event-2") || !cache.Has("event-3") {
		t.Errorf("Expected all 3 events to be present in cache")
	}
	if cache.Len() != 3 {
		t.Errorf("Expected cache length 3, got %d", cache.Len())
	}

	// Invariant 3: Eviction of least recently used item upon adding 4th item
	// Access event-1 and event-2 to make event-3 the oldest
	_ = cache.Has("event-1")
	_ = cache.Has("event-2")

	cache.Add("event-4")

	if cache.Has("event-3") {
		t.Errorf("Expected event-3 to have been evicted")
	}
	if !cache.Has("event-4") {
		t.Errorf("Expected event-4 to be present")
	}
	if cache.Len() != 3 {
		t.Errorf("Expected cache length to remain 3 after eviction, got %d", cache.Len())
	}
}

// TestLRUConcurrentAccess verifies thread-safety under heavy concurrent goroutine load.
func TestLRUConcurrentAccess(t *testing.T) {
	cache := NewLRUDeduplicationCache(1000)
	var wg sync.WaitGroup

	numGoroutines := 16
	opsPerGoroutine := 500

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("goroutine-%d-event-%d", routineID, i)
				cache.Add(key)
				_ = cache.Has(key)
			}
		}(g)
	}

	wg.Wait()

	if cache.Len() > 1000 {
		t.Errorf("Cache exceeded max capacity: %d", cache.Len())
	}
}

// TestTelemetryPayloadParsing tests deserialization of JSON telemetry events.
func TestTelemetryPayloadParsing(t *testing.T) {
	rawJSON := `{
		"event_id": "evt-test-12345",
		"timestamp": "2026-03-30T10:00:00Z",
		"service_name": "payment-service",
		"level": "ERROR",
		"message": "Database query timeout exceeding 50ms",
		"trace_id": "0123456789abcdef0123456789abcdef",
		"span_id": "0123456789abcdef",
		"http_status": 500,
		"duration_ms": 52.4,
		"metadata": {
			"endpoint": "/api/v1/payments/charge",
			"account_id": "acc_9876"
		}
	}`

	var record LogRecord
	err := json.Unmarshal([]byte(rawJSON), &record)
	if err != nil {
		t.Fatalf("Failed to unmarshal LogRecord: %v", err)
	}

	if record.EventID != "evt-test-12345" {
		t.Errorf("Expected event_id evt-test-12345, got %s", record.EventID)
	}
	if record.ServiceName != "payment-service" {
		t.Errorf("Expected service_name payment-service, got %s", record.ServiceName)
	}
	if record.HTTPStatus == nil || *record.HTTPStatus != 500 {
		t.Errorf("Expected http_status 500, got %v", record.HTTPStatus)
	}

	metaBytes := record.MetadataJSON()
	if len(metaBytes) == 0 || string(metaBytes) == "{}" {
		t.Errorf("Expected non-empty metadata JSON, got %s", string(metaBytes))
	}

	parsedTime := record.ParsedTimestamp()
	if parsedTime.Year() != 2026 {
		t.Errorf("Expected parsed timestamp year 2026, got %d", parsedTime.Year())
	}
}

// BenchmarkLRUDeduplication benchmarks LRU cache operations.
func BenchmarkLRUDeduplication(b *testing.B) {
	cache := NewLRUDeduplicationCache(50000)
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-key-%d", i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			k := keys[idx%len(keys)]
			cache.Add(k)
			_ = cache.Has(k)
			idx++
		}
	})
}

// BenchmarkTelemetryParsing benchmarks JSON unmarshaling of telemetry records.
func BenchmarkTelemetryParsing(b *testing.B) {
	raw := []byte(`{"event_id":"evt-bench-1","timestamp":"2026-03-30T10:00:00Z","service_name":"payment-service","level":"INFO","message":"Payment authorized","http_status":200,"duration_ms":14.2}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var rec LogRecord
		_ = json.Unmarshal(raw, &rec)
	}
}
