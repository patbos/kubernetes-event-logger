package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"

	"github.com/go-logr/logr"
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
				runInformer(ctx, clientset, loggerEvent)
			},
			OnStoppedLeading: func() {
				slog.Info("Lost leadership, shutting down.")
				cancel()
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

func runInformer(ctx context.Context, clientset *kubernetes.Clientset, loggerEvent *log.Logger) {
	factory := informers.NewSharedInformerFactory(clientset, 0)
	defer factory.Shutdown()
	eventInformer := factory.Core().V1().Events().Informer()

	startTime := time.Now().UTC()
	slog.Info("Became leader. Only logging events occurring after start time.", "start_time", startTime.Format(time.RFC3339))

	_, err := eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			event, ok := obj.(*v1.Event)
			if !ok {
				return
			}
			if isHistorical(event, startTime) {
				return
			}
			j, err := json.Marshal(obj)
			if err != nil {
				slog.Error("Failed to marshal event", "error", err)
				return
			}
			loggerEvent.Printf("%s\n", string(j))
		},
	})
	if err != nil {
		slog.Error("Failed to add event handler", "error", err)
		os.Exit(1)
	}

	slog.Info("Starting event informer...")
	factory.Start(ctx.Done())

	slog.Info("Waiting for informer caches to sync...")
	if ok := cache.WaitForCacheSync(ctx.Done(), eventInformer.HasSynced); !ok {
		slog.Error("Failed to wait for caches to sync")
		os.Exit(1)
	}
	slog.Info("Caches synced. Listening for new events...")

	<-ctx.Done()
	slog.Info("Shutting down.")
}

func isHistorical(event *v1.Event, startTime time.Time) bool {
	eventTs := event.EventTime.Time
	if eventTs.IsZero() {
		eventTs = event.LastTimestamp.Time
	}
	if eventTs.IsZero() {
		eventTs = event.FirstTimestamp.Time
	}
	return !eventTs.UTC().After(startTime)
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
