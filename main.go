package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

var version = "dev"

type eventLogEntry struct {
	Time  time.Time `json:"time"`
	Level string    `json:"level"`
	Event *v1.Event `json:"event"`
}

// Health state tracking
var (
	healthState = struct {
		sync.RWMutex
		isLeader        bool
		cacheSynced     bool
		startTime       time.Time
		leaderStartTime time.Time
	}{
		startTime: time.Now(),
	}
)

var (
	eventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kubernetes_event_logger_events_total",
			Help: "Total number of Kubernetes events received and logged.",
		},
		[]string{"type"},
	)
	leaderGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kubernetes_event_logger_leader",
		Help: "1 if this instance is the current leader, 0 otherwise.",
	})
	leaderElectionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kubernetes_event_logger_leader_elections_total",
		Help: "Total number of times this instance acquired leadership.",
	})
	lastEventTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kubernetes_event_logger_last_event_processed_timestamp_seconds",
		Help: "Unix timestamp of the last successfully processed event.",
	})
	eventsFilteredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kubernetes_event_logger_events_filtered_total",
		Help: "Total number of events filtered out before logging.",
	}, []string{"filter_type"})
	eventsFailedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kubernetes_event_logger_events_failed_total",
		Help: "Total number of events that failed to process.",
	}, []string{"reason"})
	eventProcessingDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kubernetes_event_logger_event_processing_duration_seconds",
		Help:    "Time taken to process (marshal and log) a single event.",
		Buckets: prometheus.DefBuckets,
	})
	eventsByNamespaceTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kubernetes_event_logger_events_by_namespace_total",
		Help: "Total number of events logged, broken down by namespace.",
	}, []string{"namespace"})
	eventsByReasonTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kubernetes_event_logger_events_by_reason_total",
		Help: "Total number of events logged, broken down by reason.",
	}, []string{"reason"})
	eventsByObjectKindTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kubernetes_event_logger_events_by_object_kind_total",
		Help: "Total number of events logged, broken down by involved object kind.",
	}, []string{"object_kind"})
	informerCacheSyncDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kubernetes_event_logger_informer_cache_sync_duration_seconds",
		Help: "Time taken for the informer cache to sync on startup (seconds).",
	})
)

func init() {
	prometheus.MustRegister(
		eventsTotal,
		leaderGauge,
		leaderElectionsTotal,
		lastEventTimestamp,
		eventsFilteredTotal,
		eventsFailedTotal,
		eventProcessingDuration,
		eventsByNamespaceTotal,
		eventsByReasonTotal,
		eventsByObjectKindTotal,
		informerCacheSyncDuration,
	)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var kubeconfigDefault string
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		kubeconfigDefault = filepath.Join(home, ".kube", "config")
	}

	kubeconfig := flag.String("kubeconfig", kubeconfigDefault, "(optional) absolute path to the kubeconfig file")
	leaseDuration := flag.Duration("lease-duration", 15*time.Second, "duration a leader lease is valid before another candidate can take over")
	renewDeadline := flag.Duration("renew-deadline", 10*time.Second, "duration the leader has to renew the lease before losing it")
	retryPeriod := flag.Duration("retry-period", 2*time.Second, "how often candidates retry acquiring or renewing the lease")
	enableDetailedMetrics := flag.Bool("enable-detailed-metrics", false, "enable high-cardinality metrics (events by namespace, reason, and object kind)")
	var excludeFilters eventFilters
	flag.Var(&excludeFilters, "exclude-filter", "exclude events matching all clauses in a rule; repeatable, format: field=value[,field=value] with fields namespace,kind,name,reason,type,reporting-component,reporting-controller,source-component")
	flag.Parse()

	loggerEvent := log.New(os.Stdout, "", 0)

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	klog.SetLogger(logr.FromSlogHandler(slog.Default().Handler()))

	slog.Info("Starting kubernetes-event-logger", "version", version)

	config, err := getK8sConfig(*kubeconfig)
	if err != nil {
		slog.Error("Failed to load kubernetes configuration", "error", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		slog.Error("Failed to create kubernetes clientset", "error", err)
		os.Exit(1)
	}

	id, err := os.Hostname()
	if err != nil {
		slog.Error("Failed to get hostname for leader election identity", "error", err)
		os.Exit(1)
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      "kubernetes-event-logger",
			Namespace: namespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", handleHealth)
	mux.HandleFunc("/readyz", handleHealth)
	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("metrics server shutdown error", "error", err)
		}
	}()

	// Define event processor
	logEvent := func(obj interface{}) {
		healthState.RLock()
		isLeader := healthState.isLeader
		leaderStartTime := healthState.leaderStartTime
		healthState.RUnlock()

		if !isLeader {
			return
		}

		start := time.Now()
		event, ok := obj.(*v1.Event)
		if !ok {
			return
		}
		if isHistorical(event, leaderStartTime) {
			eventsFilteredTotal.WithLabelValues("historical").Inc()
			return
		}
		if excludeFilters.Match(event) {
			eventsFilteredTotal.WithLabelValues("excluded_filter").Inc()
			return
		}
		wrapper, err := json.Marshal(eventLogEntry{
			Time:  eventTime(event),
			Level: eventLevel(event.Type),
			Event: event,
		})
		if err != nil {
			slog.Error("Failed to marshal event", "error", err)
			eventsFailedTotal.WithLabelValues("marshal_error").Inc()
			return
		}
		loggerEvent.Printf("%s\n", string(wrapper))
		eventsTotal.WithLabelValues(event.Type).Inc()
		if *enableDetailedMetrics {
			eventsByNamespaceTotal.WithLabelValues(event.InvolvedObject.Namespace).Inc()
			eventsByReasonTotal.WithLabelValues(event.Reason).Inc()
			eventsByObjectKindTotal.WithLabelValues(event.InvolvedObject.Kind).Inc()
		}
		lastEventTimestamp.SetToCurrentTime()
		eventProcessingDuration.Observe(time.Since(start).Seconds())
	}

	// Sync cache for all pods (leader and standby) before leader election
	factory := informers.NewSharedInformerFactory(clientset, 0)
	defer factory.Shutdown()

	eventInformer := factory.Core().V1().Events().Informer()
	_, err = eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: logEvent,
		UpdateFunc: func(_, newObj interface{}) {
			logEvent(newObj)
		},
	})
	if err != nil {
		slog.Error("Failed to add event handler", "error", err)
		os.Exit(1)
	}

	factory.Start(ctx.Done())
	slog.Info("Waiting for informer caches to sync...")
	syncStart := time.Now()
	if ok := cache.WaitForCacheSync(ctx.Done(), eventInformer.HasSynced); !ok {
		slog.Error("Failed to wait for caches to sync")
		os.Exit(1)
	}
	informerCacheSyncDuration.Set(time.Since(syncStart).Seconds())
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.Unlock()
	slog.Info("Caches synced. Ready for event processing...")

	var wg sync.WaitGroup
	var lastLeader string
	wg.Add(1)
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   *leaseDuration,
		RenewDeadline:   *renewDeadline,
		RetryPeriod:     *retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				defer wg.Done()
				leaderGauge.Set(1)
				startTime := time.Now().UTC()
				healthState.Lock()
				healthState.isLeader = true
				healthState.leaderStartTime = startTime
				healthState.Unlock()
				leaderElectionsTotal.Inc()
				slog.Info("Became leader. Starting to process events.", "start_time", startTime.Format(time.RFC3339))
				<-ctx.Done()
				slog.Info("Shutting down event processing.")
			},
			OnStoppedLeading: func() {
				leaderGauge.Set(0)
				healthState.Lock()
				healthState.isLeader = false
				healthState.Unlock()
				slog.Info("Lost leadership, entering standby mode.")
			},
			OnNewLeader: func(identity string) {
				if identity != id && identity != lastLeader {
					lastLeader = identity
					slog.Info("Standby mode.", "current_leader", identity)
				}
			},
		},
	})
	wg.Wait()
}

func eventTime(event *v1.Event) time.Time {
	if !event.EventTime.IsZero() {
		return event.EventTime.Time
	}
	if event.Series != nil && !event.Series.LastObservedTime.IsZero() {
		return event.Series.LastObservedTime.Time
	}
	if !event.LastTimestamp.IsZero() {
		return event.LastTimestamp.Time
	}
	return event.FirstTimestamp.Time
}

func eventLevel(eventType string) string {
	if strings.EqualFold(eventType, "Warning") {
		return "warn"
	}
	if strings.EqualFold(eventType, "Normal") {
		return "info"
	}
	return "debug"
}

func isHistorical(event *v1.Event, startTime time.Time) bool {
	return !eventTime(event).UTC().After(startTime)
}

func getK8sConfig(kubeconfig string) (*rest.Config, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("could not load kubeconfig or in-cluster config: %w", err)
		}
	}
	return config, nil
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	healthState.RLock()
	isLeader := healthState.isLeader
	cacheSynced := healthState.cacheSynced
	uptime := time.Since(healthState.startTime).Seconds()
	healthState.RUnlock()

	// Determine overall health status
	status := "healthy"
	statusCode := http.StatusOK

	// Liveness: pod is running if cache is synced (can serve or become leader)
	// Readiness: pod is ready if cache is synced (doesn't require leadership)
	// Non-leader pods are healthy and waiting to take over
	isHealthy := cacheSynced

	if !isHealthy {
		status = "not-ready"
		statusCode = http.StatusServiceUnavailable
	}

	response := map[string]interface{}{
		"status":         status,
		"leader":         isLeader,
		"cache_synced":   cacheSynced,
		"uptime_seconds": uptime,
		"version":        version,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode health response", "error", err)
	}
}
