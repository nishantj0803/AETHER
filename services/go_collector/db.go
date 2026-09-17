package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DatabaseClient manages PostgreSQL connection pooling and high-throughput batch writes.
type DatabaseClient struct {
	pool *pgxpool.Pool
}

// NewDatabaseClient initializes the pgx connection pool with sensible production defaults.
func NewDatabaseClient(ctx context.Context, databaseURL string) (*DatabaseClient, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	config.MinConns = 4
	config.MaxConns = 32
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		log.Printf("[WARN] PostgreSQL ping failed (%v). Operating with lazy connection.", err)
	} else {
		safeURL := regexp.MustCompile(`:([^@]+)@`).ReplaceAllString(databaseURL, ":****@")
		log.Printf("[INFO] Connected to PostgreSQL at %s (minConns=%d, maxConns=%d)", safeURL, config.MinConns, config.MaxConns)
	}

	return &DatabaseClient{pool: pool}, nil
}

// Close gracefully terminates all pool connections.
func (db *DatabaseClient) Close() {
	if db.pool != nil {
		db.pool.Close()
		log.Println("[INFO] Database connection pool closed")
	}
}

// InsertBatch executes a pipelined bulk insert of telemetry log records using pgx.Batch.
// Returns the count of newly inserted records and any error encountered.
func (db *DatabaseClient) InsertBatch(ctx context.Context, records []*LogRecord) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	start := time.Now()
	batch := &pgx.Batch{}

	query := `
		INSERT INTO telemetry_logs (
			event_id, timestamp, service_name, level, message,
			trace_id, span_id, http_status, duration_ms, metadata
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10
		) ON CONFLICT (event_id) DO NOTHING;
	`

	for _, rec := range records {
		batch.Queue(
			query,
			rec.EventID,
			rec.ParsedTimestamp(),
			rec.ServiceName,
			rec.Level,
			rec.Message,
			rec.TraceID,
			rec.SpanID,
			rec.HTTPStatus,
			rec.DurationMS,
			rec.MetadataJSON(),
		)
	}

	results := db.pool.SendBatch(ctx, batch)
	defer results.Close()

	insertedCount := 0
	for i := 0; i < len(records); i++ {
		cmdTag, err := results.Exec()
		if err != nil {
			return insertedCount, fmt.Errorf("error executing batch row %d: %w", i, err)
		}
		if cmdTag.RowsAffected() > 0 {
			insertedCount++
		}
	}

	elapsed := time.Since(start).Seconds()
	batchDuration.Observe(elapsed)
	batchSizeHist.Observe(float64(len(records)))

	return insertedCount, nil
}
