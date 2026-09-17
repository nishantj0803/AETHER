package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type pendingItem struct {
	record  *LogRecord
	message kafka.Message
}

// Consumer manages the multi-threaded ingestion pipeline from Redpanda/Kafka to PostgreSQL.
type Consumer struct {
	config      *Config
	db          *DatabaseClient
	dedupCache  *LRUDeduplicationCache
	reader      *kafka.Reader
	dlqWriter   *kafka.Writer

	rawChan     chan kafka.Message
	pendingChan chan pendingItem

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewConsumer initializes the consumer, channels, and Kafka connections.
func NewConsumer(ctx context.Context, cfg *Config, db *DatabaseClient, dedup *LRUDeduplicationCache) *Consumer {
	cCtx, cancel := context.WithCancel(ctx)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.TopicLogs,
		GroupID:        cfg.ConsumerGroup,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
	})

	dlqWriter := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.TopicDLQ,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}

	return &Consumer{
		config:      cfg,
		db:          db,
		dedupCache:  dedup,
		reader:      reader,
		dlqWriter:   dlqWriter,
		rawChan:     make(chan kafka.Message, cfg.BatchSize*4),
		pendingChan: make(chan pendingItem, cfg.BatchSize*4),
		ctx:         cCtx,
		cancel:      cancel,
	}
}

// Start launches the fetcher, worker pool, and batch flusher goroutines.
func (c *Consumer) Start() {
	log.Printf("[INFO] Starting Go Collector consumer on topic '%s' with %d workers (batch size=%d, timeout=%dms)",
		c.config.TopicLogs, c.config.WorkerCount, c.config.BatchSize, c.config.BatchTimeoutMS)

	// 1. Start Kafka message fetcher
	c.wg.Add(1)
	go c.fetchLoop()

	// 2. Start worker pool
	var workerWg sync.WaitGroup
	for i := 0; i < c.config.WorkerCount; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			c.workerLoop(workerID)
		}(i)
	}
	activeWorkersGauge.Set(float64(c.config.WorkerCount))

	// Close pendingChan once all workers finish
	go func() {
		workerWg.Wait()
		close(c.pendingChan)
	}()

	// 3. Start batch database flusher
	c.wg.Add(1)
	go c.batchFlushLoop()
}

// Stop initiates graceful shutdown, closes readers, and waits for goroutines to drain.
func (c *Consumer) Stop() {
	log.Println("[INFO] Stopping Go Collector consumer...")
	c.cancel()
	c.wg.Wait()

	if err := c.reader.Close(); err != nil {
		log.Printf("[WARN] Error closing Kafka reader: %v", err)
	}
	if err := c.dlqWriter.Close(); err != nil {
		log.Printf("[WARN] Error closing DLQ writer: %v", err)
	}
	log.Println("[INFO] Go Collector stopped cleanly")
}

// fetchLoop continuously reads raw messages from Kafka and feeds the buffered channel.
func (c *Consumer) fetchLoop() {
	defer c.wg.Done()
	defer close(c.rawChan)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			msg, err := c.reader.FetchMessage(c.ctx)
			if err != nil {
				if c.ctx.Err() != nil {
					return
				}
				log.Printf("[ERROR] Kafka fetch error: %v. Retrying...", err)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			c.rawChan <- msg
		}
	}
}

// workerLoop parses JSON, validates required fields, performs deduplication checks, and enqueues paired records.
func (c *Consumer) workerLoop(workerID int) {
	for msg := range c.rawChan {
		var record LogRecord
		if err := json.Unmarshal(msg.Value, &record); err != nil {
			c.routeToDLQ(msg.Value, fmt.Sprintf("JSON parse error: %v", err))
			continue
		}

		if record.EventID == "" {
			c.routeToDLQ(msg.Value, "Missing required event_id field")
			continue
		}

		// Check deduplication cache
		if c.dedupCache.Has(record.EventID) {
			eventsTotal.WithLabelValues("deduped").Inc()
			continue
		}

		c.pendingChan <- pendingItem{
			record:  &record,
			message: msg,
		}
	}
}

// batchFlushLoop accumulates parsed records and writes them to PostgreSQL in high-throughput batches.
func (c *Consumer) batchFlushLoop() {
	defer c.wg.Done()

	batch := make([]*LogRecord, 0, c.config.BatchSize)
	messagesToCommit := make([]kafka.Message, 0, c.config.BatchSize)
	ticker := time.NewTicker(time.Duration(c.config.BatchTimeoutMS) * time.Millisecond)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}

		inserted, err := c.db.InsertBatch(c.ctx, batch)
		if err != nil {
			log.Printf("[ERROR] Batch insert failed: %v. Isolating batch into DLQ...", err)
			for _, rec := range batch {
				raw, _ := json.Marshal(rec)
				c.routeToDLQ(raw, fmt.Sprintf("Batch database insert error: %v", err))
			}
		} else {
			eventsTotal.WithLabelValues("ingested").Add(float64(inserted))
			// Add inserted records to LRU deduplication cache
			for _, rec := range batch {
				c.dedupCache.Add(rec.EventID)
			}
			dedupCacheSizeGauge.Set(float64(c.dedupCache.Len()))
		}

		// Commit offsets for successfully processed batch
		if len(messagesToCommit) > 0 {
			if err := c.reader.CommitMessages(c.ctx, messagesToCommit...); err != nil {
				log.Printf("[WARN] Error committing Kafka offsets: %v", err)
			}
		}

		batch = batch[:0]
		messagesToCommit = messagesToCommit[:0]
	}

	for {
		select {
		case <-c.ctx.Done():
			flush()
			return

		case item, ok := <-c.pendingChan:
			if !ok {
				flush()
				return
			}
			batch = append(batch, item.record)
			messagesToCommit = append(messagesToCommit, item.message)

			if len(batch) >= c.config.BatchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

// routeToDLQ sends malformed messages to the dead-letter queue.
func (c *Consumer) routeToDLQ(raw []byte, reason string) {
	eventsTotal.WithLabelValues("dlq").Inc()
	dlqPayload := map[string]interface{}{
		"error_reason":   reason,
		"raw_payload":    string(raw),
		"consumer_group": c.config.ConsumerGroup,
		"failed_at":      time.Now().UTC().Format(time.RFC3339),
	}
	bytes, _ := json.Marshal(dlqPayload)

	err := c.dlqWriter.WriteMessages(context.Background(), kafka.Message{
		Value: bytes,
	})
	if err != nil {
		log.Printf("[WARN] Failed to route message to DLQ: %v", err)
	}
}
