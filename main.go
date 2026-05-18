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
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

var version = "dev"

type eventLogEntry struct {
	Time  time.Time `json:"time"`
	Level string    `json:"level"`
	Event *v1.Event `json:"event"`
}

type flatEventLogEntry struct {
	Time                time.Time `json:"time"`
	Level               string    `json:"level"`
	Namespace           string    `json:"namespace,omitempty"`
	Kind                string    `json:"kind,omitempty"`
	Name                string    `json:"name,omitempty"`
	Reason              string    `json:"reason,omitempty"`
	Type                string    `json:"type,omitempty"`
	Message             string    `json:"message,omitempty"`
	ReportingComponent  string    `json:"reportingComponent,omitempty"`
	ReportingController string    `json:"reportingController,omitempty"`
	SourceComponent     string    `json:"sourceComponent,omitempty"`
	Count               int32     `json:"count,omitempty"`
}

type messageEventLogEntry struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Message string    `json:"message,omitempty"`
}

type eventFormatter func(event *v1.Event) any

func eventLogFormatter(format string) (eventFormatter, error) {
	switch format {
	case "legacy":
		return legacyEventLogEntry, nil
	case "flat":
		return flatEventLogEntryFor, nil
	case "message":
		return messageEventLogEntryFor, nil
	default:
		return nil, fmt.Errorf("unsupported log format %q: expected one of flat, legacy, message", format)
	}
}

func legacyEventLogEntry(event *v1.Event) any {
	return eventLogEntry{
		Time:  eventTime(event),
		Level: eventLevel(event.Type),
		Event: event,
	}
}

func flatEventLogEntryFor(event *v1.Event) any {
	return flatEventLogEntry{
		Time:                eventTime(event),
		Level:               eventLevel(event.Type),
		Namespace:           event.InvolvedObject.Namespace,
		Kind:                event.InvolvedObject.Kind,
		Name:                event.InvolvedObject.Name,
		Reason:              event.Reason,
		Type:                event.Type,
		Message:             event.Message,
		ReportingComponent:  eventReportingComponent(event),
		ReportingController: event.ReportingController,
		SourceComponent:     event.Source.Component,
		Count:               event.Count,
	}
}

func messageEventLogEntryFor(event *v1.Event) any {
	return messageEventLogEntry{
		Time:    eventTime(event),
		Level:   eventLevel(event.Type),
		Message: event.Message,
	}
}

type leaderStatusFunc func() (bool, time.Time)

type eventProcessor struct {
	leaderStatus   leaderStatusFunc
	excludeFilters eventFilters
	logger         *log.Logger
	metrics        eventProcessorMetrics
	format         eventFormatter
	marshal        func(v any) ([]byte, error)
	now            func() time.Time
}

type eventProcessorMetrics interface {
	eventLogged(event *v1.Event)
	eventFiltered(filterType string)
	eventFailed(reason string)
	observeProcessingDuration(duration time.Duration)
}

// leaderCallbackMetrics is the subset of metrics used by leaderCallbacks.
// Defined as an interface so tests can substitute fakes without touching
// the Prometheus registry.
type leaderCallbackMetrics interface {
	setLeaderGauge(value float64)
	incLeaderElections()
}

// leaderCallbacks adapts the leaderelection.LeaderCallbacks interface onto
// the application's tracker and metrics. The methods are designed to be
// race-free with respect to the client-go contract:
//   - OnStartedLeading is invoked in a goroutine by client-go.
//   - OnStoppedLeading is invoked synchronously via defer inside Run, so it
//     always runs before RunOrDie returns.
//   - wasLeader bridges the two: setting it from OnStartedLeading and
//     reading it from OnStoppedLeading lets the stop path know whether the
//     start path actually ran. atomic.Bool is used because the two
//     callbacks may race on machines where OnStartedLeading is scheduled
//     after OnStoppedLeading begins running.
type leaderCallbacks struct {
	tracker    *healthTracker
	metrics    leaderCallbackMetrics
	identity   string
	now        func() time.Time
	wasLeader  atomic.Bool
	lastLeader string
}

func newLeaderCallbacks(tracker *healthTracker, metrics leaderCallbackMetrics, identity string) *leaderCallbacks {
	return &leaderCallbacks{
		tracker:  tracker,
		metrics:  metrics,
		identity: identity,
		now:      func() time.Time { return time.Now().UTC() },
	}
}

func (c *leaderCallbacks) OnStartedLeading(_ context.Context) {
	c.wasLeader.Store(true)
	c.metrics.setLeaderGauge(1)
	startTime := c.now()
	c.tracker.setLeader(true, startTime)
	c.metrics.incLeaderElections()
	slog.Info("Became leader. Starting to process events.", "start_time", startTime.Format(time.RFC3339))
}

func (c *leaderCallbacks) OnStoppedLeading() {
	c.metrics.setLeaderGauge(0)
	c.tracker.setLeader(false, time.Time{})
	if c.wasLeader.Load() {
		slog.Info("Shutting down event processing.")
	}
	slog.Info("Lost leadership, entering standby mode.")
}

// OnNewLeader is invoked by client-go in a goroutine but only ever from a
// single goroutine per LeaderElector instance, so lastLeader needs no
// synchronization.
func (c *leaderCallbacks) OnNewLeader(identity string) {
	if identity == c.identity || identity == c.lastLeader {
		return
	}
	c.lastLeader = identity
	slog.Info("Standby mode.", "current_leader", identity)
}

// healthTracker tracks pod health and leader state.
// All fields are protected by mu; callers must use the provided methods.
type healthTracker struct {
	mu              sync.RWMutex
	isLeader        bool
	cacheSynced     bool
	startTime       time.Time
	leaderStartTime time.Time
}

func newHealthTracker() *healthTracker {
	return &healthTracker{startTime: time.Now()}
}

func (h *healthTracker) setLeader(isLeader bool, startTime time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.isLeader = isLeader
	if isLeader {
		h.leaderStartTime = startTime
	}
}

func (h *healthTracker) setCacheSynced() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cacheSynced = true
}

func (h *healthTracker) leaderStatus() (bool, time.Time) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.isLeader, h.leaderStartTime
}

func (h *healthTracker) handleHealth(w http.ResponseWriter, _ *http.Request) {
	h.mu.RLock()
	isLeader := h.isLeader
	cacheSynced := h.cacheSynced
	uptime := time.Since(h.startTime).Seconds()
	h.mu.RUnlock()

	status := "healthy"
	statusCode := http.StatusOK
	if !cacheSynced {
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

// appMetrics holds all Prometheus metrics for the application.
type appMetrics struct {
	eventsTotal               *prometheus.CounterVec
	leaderGauge               prometheus.Gauge
	leaderElectionsTotal      prometheus.Counter
	lastEventTimestamp        prometheus.Gauge
	eventsFilteredTotal       *prometheus.CounterVec
	eventsFailedTotal         *prometheus.CounterVec
	eventProcessingDuration   prometheus.Histogram
	informerCacheSyncDuration prometheus.Gauge
}

func newAppMetrics(reg prometheus.Registerer) *appMetrics {
	m := &appMetrics{
		eventsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "kubernetes_event_logger_events_total",
				Help: "Total number of Kubernetes events received and logged.",
			},
			[]string{"type"},
		),
		leaderGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kubernetes_event_logger_leader",
			Help: "1 if this instance is the current leader, 0 otherwise.",
		}),
		leaderElectionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "kubernetes_event_logger_leader_elections_total",
			Help: "Total number of times this instance acquired leadership.",
		}),
		lastEventTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kubernetes_event_logger_last_event_processed_timestamp_seconds",
			Help: "Unix timestamp of the last successfully processed event.",
		}),
		eventsFilteredTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kubernetes_event_logger_events_filtered_total",
			Help: "Total number of events filtered out before logging.",
		}, []string{"filter_type"}),
		eventsFailedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kubernetes_event_logger_events_failed_total",
			Help: "Total number of events that failed to process.",
		}, []string{"reason"}),
		eventProcessingDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kubernetes_event_logger_event_processing_duration_seconds",
			Help:    "Time taken to process (marshal and log) a single event.",
			Buckets: prometheus.DefBuckets,
		}),
		informerCacheSyncDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kubernetes_event_logger_informer_cache_sync_duration_seconds",
			Help: "Time taken for the informer cache to sync on startup (seconds).",
		}),
	}
	reg.MustRegister(
		m.eventsTotal,
		m.leaderGauge,
		m.leaderElectionsTotal,
		m.lastEventTimestamp,
		m.eventsFilteredTotal,
		m.eventsFailedTotal,
		m.eventProcessingDuration,
		m.informerCacheSyncDuration,
	)
	return m
}

type prometheusEventProcessorMetrics struct {
	m *appMetrics
}

func (p prometheusEventProcessorMetrics) eventLogged(event *v1.Event) {
	p.m.eventsTotal.WithLabelValues(event.Type).Inc()
	p.m.lastEventTimestamp.SetToCurrentTime()
}

func (p prometheusEventProcessorMetrics) eventFiltered(filterType string) {
	p.m.eventsFilteredTotal.WithLabelValues(filterType).Inc()
}

func (p prometheusEventProcessorMetrics) eventFailed(reason string) {
	p.m.eventsFailedTotal.WithLabelValues(reason).Inc()
}

func (p prometheusEventProcessorMetrics) observeProcessingDuration(duration time.Duration) {
	p.m.eventProcessingDuration.Observe(duration.Seconds())
}

type prometheusLeaderCallbackMetrics struct {
	m *appMetrics
}

func (p prometheusLeaderCallbackMetrics) setLeaderGauge(value float64) {
	p.m.leaderGauge.Set(value)
}

func (p prometheusLeaderCallbackMetrics) incLeaderElections() {
	p.m.leaderElectionsTotal.Inc()
}

func newEventProcessor(
	leaderStatus leaderStatusFunc,
	excludeFilters eventFilters,
	logger *log.Logger,
	metrics eventProcessorMetrics,
	format eventFormatter,
) *eventProcessor {
	if format == nil {
		format = flatEventLogEntryFor
	}
	return &eventProcessor{
		leaderStatus:   leaderStatus,
		excludeFilters: excludeFilters,
		logger:         logger,
		metrics:        metrics,
		format:         format,
		marshal:        json.Marshal,
		now:            time.Now,
	}
}

func (p *eventProcessor) process(obj interface{}) {
	isLeader, leaderStartTime := p.leaderStatus()
	if !isLeader {
		return
	}

	event, ok := obj.(*v1.Event)
	if !ok {
		return
	}

	start := p.now()
	if isHistorical(event, leaderStartTime) {
		p.metrics.eventFiltered("historical")
		return
	}
	if p.excludeFilters.Match(event) {
		p.metrics.eventFiltered("excluded_filter")
		return
	}

	wrapper, err := p.marshal(p.format(event))
	if err != nil {
		slog.Error("Failed to marshal event", "error", err)
		p.metrics.eventFailed("marshal_error")
		return
	}

	p.logger.Printf("%s\n", string(wrapper))
	p.metrics.eventLogged(event)
	p.metrics.observeProcessingDuration(p.now().Sub(start))
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	var kubeconfigDefault string
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		if p := filepath.Join(home, ".kube", "config"); fileExists(p) {
			kubeconfigDefault = p
		}
	}

	fs := flag.NewFlagSet("kubernetes-event-logger", flag.ContinueOnError)
	kubeconfig := fs.String("kubeconfig", kubeconfigDefault, "(optional) absolute path to the kubeconfig file")
	leaseDuration := fs.Duration("lease-duration", 15*time.Second, "duration a leader lease is valid before another candidate can take over")
	renewDeadline := fs.Duration("renew-deadline", 10*time.Second, "duration the leader has to renew the lease before losing it")
	retryPeriod := fs.Duration("retry-period", 2*time.Second, "how often candidates retry acquiring or renewing the lease")
	leaseName := fs.String("lease-name", "kubernetes-event-logger", "name of the leader election Lease resource")
	healthAddr := fs.String("health-addr", ":8080", "address for HTTP health endpoints")
	metricsAddr := fs.String("metrics-addr", ":9090", "address for Prometheus metrics endpoint")
	logFormat := fs.String("log-format", "flat", "event JSON log format: flat, legacy, or message")
	var excludeFilters eventFilters
	fs.Var(&excludeFilters, "exclude-filter", "exclude events matching all clauses in a rule; repeatable, format: field=value[,field=value] with fields namespace,kind,name,reason,type,reporting-component,reporting-controller,source-component. Values support shell-style wildcards (e.g. namespace=kube-*); patterns use Go path.Match syntax")
	if err := fs.Parse(args); err != nil {
		return err
	}
	format, err := eventLogFormatter(*logFormat)
	if err != nil {
		return err
	}

	loggerEvent := log.New(os.Stdout, "", 0)

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	klog.SetLogger(logr.FromSlogHandler(slog.Default().Handler()))

	slog.Info("Starting kubernetes-event-logger", "version", version)

	config, err := getK8sConfig(*kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to load kubernetes configuration: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	id, err := leaderElectionIdentity()
	if err != nil {
		return err
	}

	namespace := os.Getenv("POD_NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      *leaseName,
			Namespace: namespace,
		},
		Client: clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	reg := prometheus.NewRegistry()
	metrics := newAppMetrics(reg)
	tracker := newHealthTracker()

	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", tracker.handleHealth)
	healthMux.HandleFunc("/readyz", tracker.handleHealth)
	healthSrv := &http.Server{
		Addr:              *healthAddr,
		Handler:           healthMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("health server failed", "error", err)
		}
	}()
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsSrv := &http.Server{
		Addr:              *metricsAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server failed", "error", err)
		}
	}()
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := healthSrv.Shutdown(shutdownCtx); err != nil {
			slog.Error("health server shutdown error", "error", err)
		}
		if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
			slog.Error("metrics server shutdown error", "error", err)
		}
	}()

	eventProcessor := newEventProcessor(
		tracker.leaderStatus,
		excludeFilters,
		loggerEvent,
		prometheusEventProcessorMetrics{m: metrics},
		format,
	)

	// Sync cache for all pods (leader and standby) before leader election
	factory := informers.NewSharedInformerFactory(clientset, 0)
	defer factory.Shutdown()

	eventInformer := factory.Core().V1().Events().Informer()
	_, err = eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: eventProcessor.process,
		UpdateFunc: func(_, newObj interface{}) {
			eventProcessor.process(newObj)
		},
	})
	if err != nil {
		return fmt.Errorf("failed to add event handler: %w", err)
	}

	factory.Start(ctx.Done())
	slog.Info("Waiting for informer caches to sync...")
	syncStart := time.Now()
	if ok := cache.WaitForCacheSync(ctx.Done(), eventInformer.HasSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}
	metrics.informerCacheSyncDuration.Set(time.Since(syncStart).Seconds())
	tracker.setCacheSynced()
	slog.Info("Caches synced. Ready for event processing...")

	callbacks := newLeaderCallbacks(tracker, prometheusLeaderCallbackMetrics{m: metrics}, id)
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   *leaseDuration,
		RenewDeadline:   *renewDeadline,
		RetryPeriod:     *retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: callbacks.OnStartedLeading,
			OnStoppedLeading: callbacks.OnStoppedLeading,
			OnNewLeader:      callbacks.OnNewLeader,
		},
	})
	return nil
}

func leaderElectionIdentity() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("failed to get hostname for leader election identity: %w", err)
	}
	return hostname + "_" + uuid.NewString(), nil
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
	return "info"
}

func isHistorical(event *v1.Event, startTime time.Time) bool {
	return !eventTime(event).UTC().After(startTime)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func getK8sConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}
