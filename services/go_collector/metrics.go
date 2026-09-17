package main

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	metricsOnce sync.Once

	eventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aether_collector_events_total",
			Help: "Total count of telemetry events processed by the Go collector",
		},
		[]string{"status"}, // "ingested", "deduped", "dlq"
	)

	batchDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aether_collector_batch_duration_seconds",
			Help:    "Time taken to flush a batch of records to PostgreSQL",
			Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
	)

	batchSizeHist = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aether_collector_batch_size",
			Help:    "Distribution of batch sizes flushed to PostgreSQL",
			Buckets: []float64{10, 50, 100, 250, 500, 1000, 2000},
		},
	)

	activeWorkersGauge = prometheus.NewGauge(
		prometheus.NewGaugeOpts{
			Name: "aether_collector_active_workers",
			Help: "Number of active concurrent consumer worker goroutines",
		},
	)

	dedupCacheSizeGauge = prometheus.NewGauge(
		prometheus.NewGaugeOpts{
			Name: "aether_collector_dedup_cache_size",
			Help: "Current count of active event_ids in the LRU deduplication cache",
		},
	)
)

func initMetrics() {
	metricsOnce.Do(func() {
		prometheus.MustRegister(eventsTotal)
		prometheus.MustRegister(batchDuration)
		prometheus.MustRegister(batchSizeHist)
		prometheus.MustRegister(activeWorkersGauge)
		prometheus.MustRegister(dedupCacheSizeGauge)
	})
}

func startMetricsServer(port string) *http.Server {
	initMetrics()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy","service":"go-collector"}`))
	})

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Logged in main
		}
	}()

	return server
}
