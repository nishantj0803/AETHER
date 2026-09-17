package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if val := os.Getenv(key); val != "" {
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return defaultVal
}

func loadConfig() *Config {
	brokersStr := getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
	brokers := strings.Split(brokersStr, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	workerCount := getEnvInt("WORKER_COUNT", runtime.NumCPU()*2)
	if workerCount < 2 {
		workerCount = 4
	}

	return &Config{
		KafkaBrokers:   brokers,
		TopicLogs:      getEnv("KAFKA_TOPIC_LOGS", "telemetry.logs"),
		TopicDLQ:       getEnv("KAFKA_TOPIC_DLQ", "telemetry.dlq"),
		ConsumerGroup:  getEnv("KAFKA_CONSUMER_GROUP", "aether-go-collector"),
		DatabaseURL:    getEnv("DATABASE_URL", "postgresql://aether_user:aether_password@localhost:5432/aether_db"),
		WorkerCount:    workerCount,
		BatchSize:      getEnvInt("BATCH_SIZE", 500),
		BatchTimeoutMS: getEnvInt("BATCH_TIMEOUT_MS", 50),
		DedupCapacity:  getEnvInt("DEDUP_CAPACITY", 50000),
		PrometheusPort: getEnv("PROMETHEUS_PORT", "9102"),
	}
}

func main() {
	log.Println("==========================================================")
	log.Println("  Aether High-Throughput Go Ingestion Collector v1.0.0")
	log.Println("==========================================================")

	cfg := loadConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize Prometheus metrics HTTP server
	metricsServer := startMetricsServer(cfg.PrometheusPort)
	log.Printf("[INFO] Metrics & health endpoints listening on :%s", cfg.PrometheusPort)

	// 2. Initialize PostgreSQL connection pool
	db, err := NewDatabaseClient(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize database: %v", err)
	}
	defer db.Close()

	// 3. Initialize LRU deduplication cache
	dedupCache := NewLRUDeduplicationCache(cfg.DedupCapacity)
	log.Printf("[INFO] LRU deduplication cache initialized with capacity %d", cfg.DedupCapacity)

	// 4. Start Kafka consumer
	consumer := NewConsumer(ctx, cfg, db, dedupCache)
	consumer.Start()

	// 5. Block on OS termination signals for zero-loss graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan
	log.Printf("[INFO] Received signal '%v'. Initiating graceful shutdown...", sig)

	// Stop consumer and drain in-flight batches
	consumer.Stop()

	// Shutdown metrics server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("[WARN] Metrics server shutdown error: %v", err)
	}

	log.Println("[INFO] Aether Go Collector terminated successfully")
}
