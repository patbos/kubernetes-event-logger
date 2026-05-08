package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fakeEventProcessorMetrics struct {
	loggedEvents    []*v1.Event
	detailedMetrics []bool
	filtered        map[string]int
	failed          map[string]int
	durations       []time.Duration
}

func newFakeEventProcessorMetrics() *fakeEventProcessorMetrics {
	return &fakeEventProcessorMetrics{
		filtered: map[string]int{},
		failed:   map[string]int{},
	}
}

func (m *fakeEventProcessorMetrics) eventLogged(event *v1.Event, detailedMetrics bool) {
	m.loggedEvents = append(m.loggedEvents, event)
	m.detailedMetrics = append(m.detailedMetrics, detailedMetrics)
}

func (m *fakeEventProcessorMetrics) eventFiltered(filterType string) {
	m.filtered[filterType]++
}

func (m *fakeEventProcessorMetrics) eventFailed(reason string) {
	m.failed[reason]++
}

func (m *fakeEventProcessorMetrics) observeProcessingDuration(duration time.Duration) {
	m.durations = append(m.durations, duration)
}

func newTestEventProcessor(
	leaderStatus leaderStatusFunc,
	excludeFilters eventFilters,
	detailedMetrics bool,
	metrics *fakeEventProcessorMetrics,
	output *bytes.Buffer,
) *eventProcessor {
	return &eventProcessor{
		leaderStatus:    leaderStatus,
		excludeFilters:  excludeFilters,
		logger:          log.New(output, "", 0),
		detailedMetrics: detailedMetrics,
		metrics:         metrics,
		format:          legacyEventLogEntry,
		marshal:         json.Marshal,
		now: func() time.Time {
			return time.Unix(100, 0).UTC()
		},
	}
}

func TestEventTime(t *testing.T) {
	now := time.Now().UTC()
	past := now.Add(-1 * time.Hour)
	future := now.Add(1 * time.Hour)

	testCases := []struct {
		name     string
		event    *v1.Event
		expected time.Time
	}{
		{
			name: "prefers EventTime over other timestamps",
			event: &v1.Event{
				EventTime:      metav1.MicroTime{Time: now},
				LastTimestamp:  metav1.Time{Time: past},
				FirstTimestamp: metav1.Time{Time: past},
			},
			expected: now,
		},
		{
			name: "falls back to Series.LastObservedTime for ongoing event series",
			event: &v1.Event{
				LastTimestamp:  metav1.Time{Time: past},
				FirstTimestamp: metav1.Time{Time: past},
				Series: &v1.EventSeries{
					LastObservedTime: metav1.MicroTime{Time: now},
				},
			},
			expected: now,
		},
		{
			name: "prefers Series.LastObservedTime over LastTimestamp",
			event: &v1.Event{
				LastTimestamp:  metav1.Time{Time: past},
				FirstTimestamp: metav1.Time{Time: past},
				Series: &v1.EventSeries{
					LastObservedTime: metav1.MicroTime{Time: future},
				},
			},
			expected: future,
		},
		{
			name: "falls back to LastTimestamp when EventTime and Series.LastObservedTime are zero",
			event: &v1.Event{
				LastTimestamp:  metav1.Time{Time: now},
				FirstTimestamp: metav1.Time{Time: past},
			},
			expected: now,
		},
		{
			name: "falls back to FirstTimestamp when EventTime and LastTimestamp are zero",
			event: &v1.Event{
				FirstTimestamp: metav1.Time{Time: now},
			},
			expected: now,
		},
		{
			name:     "handles all zero timestamps",
			event:    &v1.Event{},
			expected: time.Time{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := eventTime(tc.event)
			if !got.Equal(tc.expected) {
				t.Fatalf("eventTime() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestEventLevel(t *testing.T) {
	testCases := []struct {
		name      string
		eventType string
		expected  string
	}{
		{name: "Warning", eventType: "Warning", expected: "warn"},
		{name: "Normal", eventType: "Normal", expected: "info"},
		{name: "Unknown", eventType: "Unknown", expected: "debug"},
		{name: "empty string", eventType: "", expected: "debug"},
		{name: "Custom", eventType: "Custom", expected: "debug"},
		{name: "warning (lowercase)", eventType: "warning", expected: "warn"},
		{name: "WARNING (uppercase)", eventType: "WARNING", expected: "warn"},
		{name: "normal (lowercase)", eventType: "normal", expected: "info"},
		{name: "NORMAL (uppercase)", eventType: "NORMAL", expected: "info"},
		{name: "Error", eventType: "Error", expected: "debug"},
		{name: "Critical", eventType: "Critical", expected: "debug"},
		{name: "Info", eventType: "Info", expected: "debug"},
		{name: "Notice", eventType: "Notice", expected: "debug"},
		{name: "Debug", eventType: "Debug", expected: "debug"},
		{name: "Trace", eventType: "Trace", expected: "debug"},
		{name: "random-string", eventType: "random-string", expected: "debug"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := eventLevel(tc.eventType)
			if got != tc.expected {
				t.Fatalf("eventLevel(%q) = %q, want %q", tc.eventType, got, tc.expected)
			}
		})
	}
}

func TestIsHistorical(t *testing.T) {
	now := time.Now().UTC()
	past := now.Add(-1 * time.Hour)
	future := now.Add(1 * time.Hour)

	testCases := []struct {
		name      string
		event     *v1.Event
		startTime time.Time
		expected  bool
	}{
		{
			name:      "event before startTime is historical",
			event:     &v1.Event{EventTime: metav1.MicroTime{Time: past}},
			startTime: now,
			expected:  true,
		},
		{
			name:      "event after startTime is not historical",
			event:     &v1.Event{EventTime: metav1.MicroTime{Time: future}},
			startTime: now,
			expected:  false,
		},
		{
			name:      "event at exact startTime is historical",
			event:     &v1.Event{EventTime: metav1.MicroTime{Time: now}},
			startTime: now,
			expected:  true,
		},
		{
			name: "uses FirstTimestamp when EventTime is zero",
			event: &v1.Event{
				FirstTimestamp: metav1.Time{Time: past},
			},
			startTime: now,
			expected:  true,
		},
		{
			name: "uses Series.LastObservedTime for ongoing event series",
			event: &v1.Event{
				FirstTimestamp: metav1.Time{Time: past},
				Series: &v1.EventSeries{
					LastObservedTime: metav1.MicroTime{Time: future},
				},
			},
			startTime: now,
			expected:  false,
		},
		{
			name:      "nanosecond before startTime is historical",
			event:     &v1.Event{EventTime: metav1.MicroTime{Time: now.Add(-1 * time.Nanosecond)}},
			startTime: now,
			expected:  true,
		},
		{
			name:      "microsecond before startTime is historical",
			event:     &v1.Event{EventTime: metav1.MicroTime{Time: now.Add(-1 * time.Microsecond)}},
			startTime: now,
			expected:  true,
		},
		{
			name:      "millisecond after startTime is not historical",
			event:     &v1.Event{EventTime: metav1.MicroTime{Time: now.Add(1 * time.Millisecond)}},
			startTime: now,
			expected:  false,
		},
		{
			name: "historical by EventTime even when other timestamps are future",
			event: &v1.Event{
				EventTime:      metav1.MicroTime{Time: now.Add(-1 * time.Hour)},
				LastTimestamp:  metav1.Time{Time: now.Add(1 * time.Hour)},
				FirstTimestamp: metav1.Time{Time: now.Add(2 * time.Hour)},
			},
			startTime: now,
			expected:  true,
		},
		{
			name: "historical by LastTimestamp when EventTime is zero",
			event: &v1.Event{
				LastTimestamp:  metav1.Time{Time: now.Add(-30 * time.Minute)},
				FirstTimestamp: metav1.Time{Time: now.Add(1 * time.Hour)},
			},
			startTime: now,
			expected:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := isHistorical(tc.event, tc.startTime)
			if got != tc.expected {
				t.Fatalf("isHistorical() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestEventProcessorProcessesLeaderEvent(t *testing.T) {
	var output bytes.Buffer
	metrics := newFakeEventProcessorMetrics()
	leaderStart := time.Unix(100, 0).UTC()
	eventTime := leaderStart.Add(time.Second)
	event := &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Namespace: "default",
			Kind:      "Pod",
			Name:      "pod-1",
		},
		Reason:    "Started",
		Type:      "Normal",
		EventTime: metav1.MicroTime{Time: eventTime},
	}
	processor := newTestEventProcessor(
		func() (bool, time.Time) { return true, leaderStart },
		nil,
		true,
		metrics,
		&output,
	)

	processor.process(event)

	if len(metrics.loggedEvents) != 1 {
		t.Fatalf("logged events = %d, want 1", len(metrics.loggedEvents))
	}
	if !metrics.detailedMetrics[0] {
		t.Fatal("detailedMetrics = false, want true")
	}
	if len(metrics.durations) != 1 {
		t.Fatalf("durations = %d, want 1", len(metrics.durations))
	}
	if output.Len() == 0 {
		t.Fatal("processor did not write log output")
	}

	var entry eventLogEntry
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &entry); err != nil {
		t.Fatalf("failed to unmarshal log output: %v", err)
	}
	if entry.Level != "info" {
		t.Fatalf("entry.Level = %q, want info", entry.Level)
	}
	if !entry.Time.Equal(eventTime) {
		t.Fatalf("entry.Time = %v, want %v", entry.Time, eventTime)
	}
	if entry.Event == nil || entry.Event.Reason != "Started" {
		t.Fatalf("entry.Event.Reason = %v, want Started", entry.Event)
	}
}

func TestEventProcessorSkipsWhenNotLeader(t *testing.T) {
	var output bytes.Buffer
	metrics := newFakeEventProcessorMetrics()
	processor := newTestEventProcessor(
		func() (bool, time.Time) { return false, time.Now() },
		nil,
		false,
		metrics,
		&output,
	)

	processor.process(&v1.Event{Type: "Normal"})

	if output.Len() != 0 {
		t.Fatalf("log output length = %d, want 0", output.Len())
	}
	if len(metrics.loggedEvents) != 0 {
		t.Fatalf("logged events = %d, want 0", len(metrics.loggedEvents))
	}
}

func TestEventProcessorIgnoresNonEventObjects(t *testing.T) {
	var output bytes.Buffer
	metrics := newFakeEventProcessorMetrics()
	processor := newTestEventProcessor(
		func() (bool, time.Time) { return true, time.Unix(100, 0).UTC() },
		nil,
		false,
		metrics,
		&output,
	)

	processor.process("not a kubernetes event")

	if output.Len() != 0 {
		t.Fatalf("log output length = %d, want 0", output.Len())
	}
	if len(metrics.loggedEvents) != 0 {
		t.Fatalf("logged events = %d, want 0", len(metrics.loggedEvents))
	}
	if len(metrics.filtered) != 0 {
		t.Fatalf("filtered metrics = %v, want none", metrics.filtered)
	}
	if len(metrics.failed) != 0 {
		t.Fatalf("failed metrics = %v, want none", metrics.failed)
	}
	if len(metrics.durations) != 0 {
		t.Fatalf("durations = %d, want 0", len(metrics.durations))
	}
}

func TestEventProcessorPreservesDetailedMetricsFlag(t *testing.T) {
	var output bytes.Buffer
	metrics := newFakeEventProcessorMetrics()
	leaderStart := time.Unix(100, 0).UTC()
	processor := newTestEventProcessor(
		func() (bool, time.Time) { return true, leaderStart },
		nil,
		false,
		metrics,
		&output,
	)

	processor.process(&v1.Event{
		Type:      "Warning",
		EventTime: metav1.MicroTime{Time: leaderStart.Add(time.Second)},
	})

	if len(metrics.loggedEvents) != 1 {
		t.Fatalf("logged events = %d, want 1", len(metrics.loggedEvents))
	}
	if metrics.detailedMetrics[0] {
		t.Fatal("detailedMetrics = true, want false")
	}
}

func TestEventProcessorObservesProcessingDuration(t *testing.T) {
	var output bytes.Buffer
	metrics := newFakeEventProcessorMetrics()
	leaderStart := time.Unix(100, 0).UTC()
	processor := newTestEventProcessor(
		func() (bool, time.Time) { return true, leaderStart },
		nil,
		false,
		metrics,
		&output,
	)
	call := 0
	processor.now = func() time.Time {
		call++
		if call == 1 {
			return time.Unix(200, 0).UTC()
		}
		return time.Unix(202, 0).UTC()
	}

	processor.process(&v1.Event{
		Type:      "Normal",
		EventTime: metav1.MicroTime{Time: leaderStart.Add(time.Second)},
	})

	if len(metrics.durations) != 1 {
		t.Fatalf("durations = %d, want 1", len(metrics.durations))
	}
	if metrics.durations[0] != 2*time.Second {
		t.Fatalf("duration = %v, want 2s", metrics.durations[0])
	}
}

func TestEventProcessorPassesExpectedEntryToMarshal(t *testing.T) {
	var output bytes.Buffer
	metrics := newFakeEventProcessorMetrics()
	leaderStart := time.Unix(100, 0).UTC()
	eventTime := leaderStart.Add(time.Second)
	event := &v1.Event{
		Type:      "Warning",
		Reason:    "BackOff",
		EventTime: metav1.MicroTime{Time: eventTime},
	}
	processor := newTestEventProcessor(
		func() (bool, time.Time) { return true, leaderStart },
		nil,
		false,
		metrics,
		&output,
	)
	marshalCalled := false
	processor.marshal = func(v any) ([]byte, error) {
		marshalCalled = true
		entry, ok := v.(eventLogEntry)
		if !ok {
			t.Fatalf("marshal input type = %T, want eventLogEntry", v)
		}
		if !entry.Time.Equal(eventTime) {
			t.Fatalf("entry.Time = %v, want %v", entry.Time, eventTime)
		}
		if entry.Level != "warn" {
			t.Fatalf("entry.Level = %q, want warn", entry.Level)
		}
		if entry.Event != event {
			t.Fatal("entry.Event did not preserve original event pointer")
		}
		return []byte(`{"ok":true}`), nil
	}

	processor.process(event)

	if !marshalCalled {
		t.Fatal("marshal was not called")
	}
	if got := output.String(); got != "{\"ok\":true}\n" {
		t.Fatalf("log output = %q, want fixed marshaled payload with newline", got)
	}
	if len(metrics.loggedEvents) != 1 {
		t.Fatalf("logged events = %d, want 1", len(metrics.loggedEvents))
	}
}

func TestEventProcessorUsesConfiguredFormatter(t *testing.T) {
	var output bytes.Buffer
	metrics := newFakeEventProcessorMetrics()
	leaderStart := time.Unix(100, 0).UTC()
	eventTime := leaderStart.Add(time.Second)
	processor := newTestEventProcessor(
		func() (bool, time.Time) { return true, leaderStart },
		nil,
		false,
		metrics,
		&output,
	)
	processor.format = flatEventLogEntryFor

	processor.process(&v1.Event{
		InvolvedObject: v1.ObjectReference{
			Namespace: "default",
			Kind:      "Pod",
			Name:      "pod-1",
		},
		Reason:              "BackOff",
		Type:                "Warning",
		Message:             "Back-off restarting failed container",
		ReportingController: "kubelet",
		Source:              v1.EventSource{Component: "node-controller"},
		Count:               3,
		EventTime:           metav1.MicroTime{Time: eventTime},
	})

	var entry flatEventLogEntry
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &entry); err != nil {
		t.Fatalf("failed to unmarshal log output: %v", err)
	}
	if entry.Level != "warn" {
		t.Fatalf("entry.Level = %q, want warn", entry.Level)
	}
	if !entry.Time.Equal(eventTime) {
		t.Fatalf("entry.Time = %v, want %v", entry.Time, eventTime)
	}
	if entry.Namespace != "default" || entry.Kind != "Pod" || entry.Name != "pod-1" {
		t.Fatalf("object fields = namespace %q kind %q name %q, want default Pod pod-1", entry.Namespace, entry.Kind, entry.Name)
	}
	if entry.Reason != "BackOff" || entry.Type != "Warning" {
		t.Fatalf("event fields = reason %q type %q, want BackOff Warning", entry.Reason, entry.Type)
	}
	if entry.Message != "Back-off restarting failed container" {
		t.Fatalf("entry.Message = %q, want Back-off restarting failed container", entry.Message)
	}
	if entry.ReportingComponent != "kubelet" || entry.ReportingController != "kubelet" || entry.SourceComponent != "node-controller" {
		t.Fatalf("reporting fields = component %q controller %q source %q, want kubelet kubelet node-controller", entry.ReportingComponent, entry.ReportingController, entry.SourceComponent)
	}
	if entry.Count != 3 {
		t.Fatalf("entry.Count = %d, want 3", entry.Count)
	}
}

func TestEventLogFormatter(t *testing.T) {
	event := &v1.Event{Type: "Normal", Message: "created"}
	testCases := []struct {
		name     string
		format   string
		wantType any
	}{
		{name: "legacy", format: "legacy", wantType: eventLogEntry{}},
		{name: "flat", format: "flat", wantType: flatEventLogEntry{}},
		{name: "message", format: "message", wantType: messageEventLogEntry{}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			formatter, err := eventLogFormatter(tc.format)
			if err != nil {
				t.Fatalf("eventLogFormatter(%q) returned error: %v", tc.format, err)
			}
			got := formatter(event)
			if fmt.Sprintf("%T", got) != fmt.Sprintf("%T", tc.wantType) {
				t.Fatalf("formatter output type = %T, want %T", got, tc.wantType)
			}
		})
	}
}

func TestEventLogFormatterRejectsUnsupportedFormat(t *testing.T) {
	_, err := eventLogFormatter("unknown")
	if err == nil {
		t.Fatal("expected unsupported format error, got nil")
	}
	if !strings.Contains(err.Error(), "flat, legacy, message") {
		t.Fatalf("error = %q, want supported formats", err.Error())
	}
}

func TestEventProcessorDoesNotMarshalFilteredEvents(t *testing.T) {
	leaderStart := time.Unix(100, 0).UTC()
	testCases := []struct {
		name           string
		event          *v1.Event
		excludeFilters eventFilters
		filterType     string
	}{
		{
			name: "historical",
			event: &v1.Event{
				Type:      "Normal",
				EventTime: metav1.MicroTime{Time: leaderStart},
			},
			filterType: "historical",
		},
		{
			name: "excluded filter",
			event: &v1.Event{
				InvolvedObject: v1.ObjectReference{Kind: "Node"},
				Type:           "Normal",
				EventTime:      metav1.MicroTime{Time: leaderStart.Add(time.Second)},
			},
			excludeFilters: eventFilters{{Kind: "Node"}},
			filterType:     "excluded_filter",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			metrics := newFakeEventProcessorMetrics()
			processor := newTestEventProcessor(
				func() (bool, time.Time) { return true, leaderStart },
				tc.excludeFilters,
				false,
				metrics,
				&output,
			)
			processor.marshal = func(any) ([]byte, error) {
				t.Fatal("marshal should not be called for filtered events")
				return nil, nil
			}

			processor.process(tc.event)

			if metrics.filtered[tc.filterType] != 1 {
				t.Fatalf("%s filtered count = %d, want 1", tc.filterType, metrics.filtered[tc.filterType])
			}
			if output.Len() != 0 {
				t.Fatalf("log output length = %d, want 0", output.Len())
			}
			if len(metrics.loggedEvents) != 0 {
				t.Fatalf("logged events = %d, want 0", len(metrics.loggedEvents))
			}
			if len(metrics.durations) != 0 {
				t.Fatalf("durations = %d, want 0", len(metrics.durations))
			}
		})
	}
}

func TestEventProcessorFiltersHistoricalEvents(t *testing.T) {
	var output bytes.Buffer
	metrics := newFakeEventProcessorMetrics()
	leaderStart := time.Unix(100, 0).UTC()
	processor := newTestEventProcessor(
		func() (bool, time.Time) { return true, leaderStart },
		nil,
		false,
		metrics,
		&output,
	)

	processor.process(&v1.Event{
		Type:      "Normal",
		EventTime: metav1.MicroTime{Time: leaderStart},
	})

	if metrics.filtered["historical"] != 1 {
		t.Fatalf("historical filtered count = %d, want 1", metrics.filtered["historical"])
	}
	if output.Len() != 0 {
		t.Fatalf("log output length = %d, want 0", output.Len())
	}
}

func TestEventProcessorAppliesExcludeFilters(t *testing.T) {
	var output bytes.Buffer
	metrics := newFakeEventProcessorMetrics()
	leaderStart := time.Unix(100, 0).UTC()
	processor := newTestEventProcessor(
		func() (bool, time.Time) { return true, leaderStart },
		eventFilters{{Namespace: "kube-system", Type: "Normal"}},
		false,
		metrics,
		&output,
	)

	processor.process(&v1.Event{
		InvolvedObject: v1.ObjectReference{Namespace: "kube-system"},
		Type:           "Normal",
		EventTime:      metav1.MicroTime{Time: leaderStart.Add(time.Second)},
	})

	if metrics.filtered["excluded_filter"] != 1 {
		t.Fatalf("excluded_filter count = %d, want 1", metrics.filtered["excluded_filter"])
	}
	if output.Len() != 0 {
		t.Fatalf("log output length = %d, want 0", output.Len())
	}
}

func TestEventProcessorRecordsMarshalFailures(t *testing.T) {
	var output bytes.Buffer
	metrics := newFakeEventProcessorMetrics()
	leaderStart := time.Unix(100, 0).UTC()
	processor := newTestEventProcessor(
		func() (bool, time.Time) { return true, leaderStart },
		nil,
		false,
		metrics,
		&output,
	)
	processor.marshal = func(any) ([]byte, error) {
		return nil, errors.New("marshal failed")
	}

	processor.process(&v1.Event{
		Type:      "Warning",
		EventTime: metav1.MicroTime{Time: leaderStart.Add(time.Second)},
	})

	if metrics.failed["marshal_error"] != 1 {
		t.Fatalf("marshal_error count = %d, want 1", metrics.failed["marshal_error"])
	}
	if output.Len() != 0 {
		t.Fatalf("log output length = %d, want 0", output.Len())
	}
	if len(metrics.loggedEvents) != 0 {
		t.Fatalf("logged events = %d, want 0", len(metrics.loggedEvents))
	}
	if len(metrics.durations) != 0 {
		t.Fatalf("durations = %d, want 0", len(metrics.durations))
	}
}

func TestCurrentLeaderStatusReadsHealthState(t *testing.T) {
	expectedLeaderStartTime := time.Unix(300, 0).UTC()
	tracker := &healthTracker{
		isLeader:        true,
		leaderStartTime: expectedLeaderStartTime,
	}

	isLeader, leaderStartTime := tracker.leaderStatus()

	if !isLeader {
		t.Fatal("isLeader = false, want true")
	}
	if !leaderStartTime.Equal(expectedLeaderStartTime) {
		t.Fatalf("leaderStartTime = %v, want %v", leaderStartTime, expectedLeaderStartTime)
	}
}

func TestHandleHealth(t *testing.T) {
	testCases := []struct {
		name             string
		isLeader         bool
		cacheSynced      bool
		expectedCode     int
		expectedFields   []string
		checkContentType bool
		checkJSON        bool
	}{
		{
			name:         "leader with synced cache is healthy",
			isLeader:     true,
			cacheSynced:  true,
			expectedCode: http.StatusOK,
			expectedFields: []string{
				`"status":"healthy"`,
				`"leader":true`,
				`"cache_synced":true`,
				`"version"`,
				`"dev"`,
				`"uptime_seconds"`,
			},
			checkContentType: true,
			checkJSON:        true,
		},
		{
			name:         "non-leader with synced cache is healthy",
			isLeader:     false,
			cacheSynced:  true,
			expectedCode: http.StatusOK,
			expectedFields: []string{
				`"status":"healthy"`,
				`"leader":false`,
				`"cache_synced":true`,
			},
		},
		{
			name:         "leader without synced cache is not ready",
			isLeader:     true,
			cacheSynced:  false,
			expectedCode: http.StatusServiceUnavailable,
			expectedFields: []string{
				`"status":"not-ready"`,
				`"cache_synced":false`,
				`"leader":true`,
			},
		},
		{
			name:         "non-leader without synced cache is not ready",
			isLeader:     false,
			cacheSynced:  false,
			expectedCode: http.StatusServiceUnavailable,
			expectedFields: []string{
				`"status":"not-ready"`,
				`"cache_synced":false`,
				`"leader":false`,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tracker := &healthTracker{
				isLeader:    tc.isLeader,
				cacheSynced: tc.cacheSynced,
				startTime:   time.Now().Add(-5 * time.Second),
			}
			req := httptest.NewRequestWithContext(context.Background(), "GET", "/healthz", nil)
			w := httptest.NewRecorder()
			tracker.handleHealth(w, req)

			if w.Code != tc.expectedCode {
				t.Fatalf("status = %d, want %d", w.Code, tc.expectedCode)
			}
			resp := w.Body.String()
			for _, field := range tc.expectedFields {
				if !strings.Contains(resp, field) {
					t.Errorf("response missing field: %s", field)
				}
			}
			if tc.checkContentType {
				ct := w.Header().Get("Content-Type")
				if ct != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", ct)
				}
			}
			if tc.checkJSON {
				var parsed map[string]interface{}
				if err := json.Unmarshal(w.Body.Bytes(), &parsed); err != nil {
					t.Fatalf("response is not valid JSON: %v", err)
				}
				for _, key := range []string{"status", "leader", "cache_synced", "uptime_seconds", "version"} {
					if _, ok := parsed[key]; !ok {
						t.Errorf("JSON response missing field: %s", key)
					}
				}
				if _, ok := parsed["status"].(string); !ok {
					t.Error("status should be string")
				}
				if _, ok := parsed["leader"].(bool); !ok {
					t.Error("leader should be boolean")
				}
				if _, ok := parsed["cache_synced"].(bool); !ok {
					t.Error("cache_synced should be boolean")
				}
				if _, ok := parsed["uptime_seconds"].(float64); !ok {
					t.Error("uptime_seconds should be number")
				}
			}
		})
	}
}

func TestEventReportingComponent(t *testing.T) {
	testCases := []struct {
		name     string
		event    *v1.Event
		expected string
	}{
		{
			name: "prefers ReportingController when set",
			event: &v1.Event{
				ReportingController: "custom-controller",
				Source: v1.EventSource{
					Component: "kubelet",
				},
			},
			expected: "custom-controller",
		},
		{
			name: "falls back to Source.Component when ReportingController is empty",
			event: &v1.Event{
				ReportingController: "",
				Source: v1.EventSource{
					Component: "kubelet",
				},
			},
			expected: "kubelet",
		},
		{
			name: "handles empty both fields",
			event: &v1.Event{
				ReportingController: "",
				Source: v1.EventSource{
					Component: "",
				},
			},
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := eventReportingComponent(tc.event)
			if got != tc.expected {
				t.Fatalf("eventReportingComponent() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestHandleHealthLeaderTransition(t *testing.T) {
	tracker := &healthTracker{cacheSynced: true, isLeader: false}

	// First request: non-leader
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/healthz", nil)
	w := httptest.NewRecorder()
	tracker.handleHealth(w, req)
	if !strings.Contains(w.Body.String(), `"leader":false`) {
		t.Error("health check 1: expected non-leader")
	}

	// Transition to leader
	tracker.setLeader(true, time.Time{})

	// Second request: leader
	req = httptest.NewRequestWithContext(context.Background(), "GET", "/healthz", nil)
	w = httptest.NewRecorder()
	tracker.handleHealth(w, req)
	if !strings.Contains(w.Body.String(), `"leader":true`) {
		t.Error("health check 2: expected leader")
	}

	// Should still be healthy in both cases
	if w.Code != http.StatusOK {
		t.Errorf("expected status OK for healthy leader, got %d", w.Code)
	}
}

func TestEventTimeWithMicroTime(t *testing.T) {
	now := time.Now().UTC()

	// Test with high-precision EventTime (MicroTime)
	event := &v1.Event{
		EventTime: metav1.MicroTime{Time: now},
	}

	got := eventTime(event)
	if !got.Equal(now) {
		t.Fatalf("eventTime with MicroTime = %v, want %v", got, now)
	}
}

func TestEventTimePriority(t *testing.T) {
	now := time.Now().UTC()
	earlier := now.Add(-30 * time.Second)
	later := now.Add(30 * time.Second)

	testCases := []struct {
		name     string
		event    *v1.Event
		expected time.Time
	}{
		{
			name: "EventTime takes priority over all",
			event: &v1.Event{
				EventTime:      metav1.MicroTime{Time: now},
				LastTimestamp:  metav1.Time{Time: later},
				FirstTimestamp: metav1.Time{Time: earlier},
			},
			expected: now,
		},
		{
			name: "LastTimestamp used when EventTime is zero but LastTimestamp is set",
			event: &v1.Event{
				EventTime:      metav1.MicroTime{},
				LastTimestamp:  metav1.Time{Time: now},
				FirstTimestamp: metav1.Time{Time: earlier},
			},
			expected: now,
		},
		{
			name: "FirstTimestamp used when both EventTime and LastTimestamp are zero",
			event: &v1.Event{
				EventTime:      metav1.MicroTime{},
				LastTimestamp:  metav1.Time{},
				FirstTimestamp: metav1.Time{Time: now},
			},
			expected: now,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := eventTime(tc.event)
			if !got.Equal(tc.expected) {
				t.Fatalf("eventTime() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestHandleHealthConcurrency(t *testing.T) {
	tracker := &healthTracker{
		cacheSynced: true,
		isLeader:    true,
		startTime:   time.Now(),
	}

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			req := httptest.NewRequestWithContext(context.Background(), "GET", "/healthz", nil)
			w := httptest.NewRecorder()
			tracker.handleHealth(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("concurrent access failed with status %d", w.Code)
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestEventReportingComponentPriority(t *testing.T) {
	testCases := []struct {
		name                string
		reportingController string
		sourceComponent     string
		expected            string
	}{
		{
			name:                "ReportingController has priority",
			reportingController: "scheduler",
			sourceComponent:     "kubelet",
			expected:            "scheduler",
		},
		{
			name:                "Source.Component used when ReportingController is empty",
			reportingController: "",
			sourceComponent:     "kubelet",
			expected:            "kubelet",
		},
		{
			name:                "Empty string when both are empty",
			reportingController: "",
			sourceComponent:     "",
			expected:            "",
		},
		{
			name:                "ReportingController priority even if Source is different",
			reportingController: "custom",
			sourceComponent:     "other",
			expected:            "custom",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := &v1.Event{
				ReportingController: tc.reportingController,
				Source: v1.EventSource{
					Component: tc.sourceComponent,
				},
			}
			got := eventReportingComponent(event)
			if got != tc.expected {
				t.Fatalf("eventReportingComponent() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestEventTimeZeroHandling(t *testing.T) {
	testCases := []struct {
		name     string
		event    *v1.Event
		expected bool
	}{
		{
			name:     "completely empty event returns zero time",
			event:    &v1.Event{},
			expected: true,
		},
		{
			name: "event with EventTime returns non-zero",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: time.Now()},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := eventTime(tc.event)
			isZero := got.IsZero()
			if isZero != tc.expected {
				t.Fatalf("eventTime().IsZero() = %v, want %v", isZero, tc.expected)
			}
		})
	}
}

func TestParseEventFilterEdgeCases(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		shouldErr bool
	}{
		{
			name:      "valid single clause",
			input:     "namespace=default",
			shouldErr: false,
		},
		{
			name:      "valid multiple clauses",
			input:     "namespace=default,kind=Pod,type=Normal",
			shouldErr: false,
		},
		{
			name:      "spaces around equals",
			input:     "namespace = default",
			shouldErr: false,
		},
		{
			name:      "spaces in value",
			input:     "kind=Custom Type",
			shouldErr: false,
		},
		{
			name:      "empty clause between commas",
			input:     "namespace=default,,kind=Pod",
			shouldErr: false,
		},
		{
			name:      "missing value",
			input:     "namespace=",
			shouldErr: true,
		},
		{
			name:      "missing key",
			input:     "=default",
			shouldErr: true,
		},
		{
			name:      "no equals",
			input:     "namespace",
			shouldErr: true,
		},
		{
			name:      "invalid field name",
			input:     "invalid_field=value",
			shouldErr: true,
		},
		{
			name:      "trailing comma",
			input:     "namespace=default,",
			shouldErr: false,
		},
		{
			name:      "leading comma",
			input:     ",namespace=default",
			shouldErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseEventFilter(tc.input)
			hasErr := err != nil
			if hasErr != tc.shouldErr {
				t.Fatalf("parseEventFilter(%q) error = %v, shouldErr = %v", tc.input, err, tc.shouldErr)
			}
		})
	}
}

func TestEventFiltersMatchMultipleRules(t *testing.T) {
	event := &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Namespace: "default",
			Kind:      "Pod",
			Name:      "test-pod",
		},
		Reason:              "Started",
		Type:                "Normal",
		ReportingController: "kubelet",
	}

	testCases := []struct {
		name    string
		filters []eventFilter
		matches bool
	}{
		{
			name:    "no rules",
			filters: []eventFilter{},
			matches: false,
		},
		{
			name: "single matching rule",
			filters: []eventFilter{
				{Namespace: "default", Kind: "Pod"},
			},
			matches: true,
		},
		{
			name: "first rule matches",
			filters: []eventFilter{
				{Namespace: "default", Kind: "Pod"},
				{Namespace: "kube-system", Kind: "Node"},
			},
			matches: true,
		},
		{
			name: "second rule matches",
			filters: []eventFilter{
				{Namespace: "kube-system", Kind: "Node"},
				{Namespace: "default", Kind: "Pod"},
			},
			matches: true,
		},
		{
			name: "no rules match",
			filters: []eventFilter{
				{Namespace: "other", Kind: "Deployment"},
				{Namespace: "kube-system", Kind: "Node"},
			},
			matches: false,
		},
		{
			name: "partial match not enough",
			filters: []eventFilter{
				{Namespace: "default", Kind: "Deployment"},
			},
			matches: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := eventFilters(tc.filters).Match(event)
			if result != tc.matches {
				t.Fatalf("Match() = %v, want %v", result, tc.matches)
			}
		})
	}
}

func TestEventFilterAllFields(t *testing.T) {
	event := &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Namespace: "production",
			Kind:      "StatefulSet",
			Name:      "database",
		},
		Reason:              "FailedScheduling",
		Type:                "Warning",
		ReportingController: "kube-scheduler",
		Source: v1.EventSource{
			Component: "scheduler",
		},
	}

	testCases := []struct {
		name    string
		filter  eventFilter
		matches bool
	}{
		{
			name: "all fields match",
			filter: eventFilter{
				Namespace:          "production",
				Kind:               "StatefulSet",
				Name:               "database",
				Reason:             "FailedScheduling",
				Type:               "Warning",
				ReportingComponent: "kube-scheduler",
				SourceComponent:    "scheduler",
			},
			matches: true,
		},
		{
			name: "namespace mismatch",
			filter: eventFilter{
				Namespace:          "staging",
				Kind:               "StatefulSet",
				Name:               "database",
				Reason:             "FailedScheduling",
				Type:               "Warning",
				ReportingComponent: "kube-scheduler",
				SourceComponent:    "scheduler",
			},
			matches: false,
		},
		{
			name: "kind mismatch",
			filter: eventFilter{
				Namespace:          "production",
				Kind:               "Deployment",
				Name:               "database",
				Reason:             "FailedScheduling",
				Type:               "Warning",
				ReportingComponent: "kube-scheduler",
				SourceComponent:    "scheduler",
			},
			matches: false,
		},
		{
			name: "only required fields",
			filter: eventFilter{
				Namespace: "production",
				Kind:      "StatefulSet",
			},
			matches: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.filter.Match(event)
			if result != tc.matches {
				t.Fatalf("Match() = %v, want %v", result, tc.matches)
			}
		})
	}
}

func TestEventTimestampVariations(t *testing.T) {
	baseTime := time.Now().UTC()

	testCases := []struct {
		name     string
		event    *v1.Event
		expected time.Time
	}{
		{
			name: "only EventTime",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: baseTime},
			},
			expected: baseTime,
		},
		{
			name: "only LastTimestamp",
			event: &v1.Event{
				LastTimestamp: metav1.Time{Time: baseTime},
			},
			expected: baseTime,
		},
		{
			name: "only FirstTimestamp",
			event: &v1.Event{
				FirstTimestamp: metav1.Time{Time: baseTime},
			},
			expected: baseTime,
		},
		{
			name: "EventTime and LastTimestamp",
			event: &v1.Event{
				EventTime:     metav1.MicroTime{Time: baseTime},
				LastTimestamp: metav1.Time{Time: baseTime.Add(1 * time.Second)},
			},
			expected: baseTime,
		},
		{
			name: "all three timestamps",
			event: &v1.Event{
				EventTime:      metav1.MicroTime{Time: baseTime},
				LastTimestamp:  metav1.Time{Time: baseTime.Add(1 * time.Second)},
				FirstTimestamp: metav1.Time{Time: baseTime.Add(2 * time.Second)},
			},
			expected: baseTime,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := eventTime(tc.event)
			if !got.Equal(tc.expected) {
				t.Fatalf("eventTime() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestEventFilterMatchingWithEmptyEvent(t *testing.T) {
	emptyEvent := &v1.Event{}

	filter := eventFilter{
		Namespace: "default",
	}

	// Empty event should not match any filter with criteria
	if filter.Match(emptyEvent) {
		t.Fatal("filter with namespace criteria should not match empty event")
	}
}

func TestEventFilterMatchingEmptyFilter(t *testing.T) {
	event := &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Namespace: "default",
			Kind:      "Pod",
		},
		Type: "Normal",
	}

	emptyFilter := eventFilter{}

	// Empty filter matches everything (no criteria to reject)
	if !emptyFilter.Match(event) {
		t.Fatal("empty filter should match all events")
	}
}

func TestEventFilterString(t *testing.T) {
	testCases := []struct {
		name     string
		filter   eventFilter
		expected string
	}{
		{
			name:     "single field",
			filter:   eventFilter{Namespace: "default"},
			expected: "namespace=default",
		},
		{
			name:     "multiple fields",
			filter:   eventFilter{Namespace: "default", Kind: "Pod", Type: "Normal"},
			expected: "namespace=default,kind=Pod,type=Normal",
		},
		{
			name: "all fields",
			filter: eventFilter{
				Namespace:          "kube-system",
				Kind:               "Node",
				Name:               "node-1",
				Reason:             "Ready",
				Type:               "Normal",
				ReportingComponent: "kubelet",
				SourceComponent:    "node-controller",
			},
			expected: "namespace=kube-system,kind=Node,name=node-1,reason=Ready,type=Normal,reporting-component=kubelet,source-component=node-controller",
		},
		{
			name:     "empty filter",
			filter:   eventFilter{},
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.filter.String()
			if got != tc.expected {
				t.Fatalf("String() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestEventFiltersString(t *testing.T) {
	testCases := []struct {
		name     string
		filters  eventFilters
		expected string
	}{
		{
			name:     "single filter",
			filters:  eventFilters{{Namespace: "default"}},
			expected: "namespace=default",
		},
		{
			name: "multiple filters",
			filters: eventFilters{
				{Namespace: "default", Kind: "Pod"},
				{Namespace: "kube-system", Kind: "Node"},
			},
			expected: "namespace=default,kind=Pod;namespace=kube-system,kind=Node",
		},
		{
			name:     "empty filters",
			filters:  eventFilters{},
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.filters.String()
			if got != tc.expected {
				t.Fatalf("String() = %q, want %q", got, tc.expected)
			}
		})
	}
}

func TestEventFiltersSet(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		shouldErr bool
	}{
		{
			name:      "valid filter",
			input:     "namespace=default,kind=Pod",
			shouldErr: false,
		},
		{
			name:      "invalid filter",
			input:     "invalid_field=value",
			shouldErr: true,
		},
		{
			name:      "empty string",
			input:     "",
			shouldErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var filters eventFilters
			err := filters.Set(tc.input)
			hasErr := err != nil
			if hasErr != tc.shouldErr {
				t.Fatalf("Set(%q) error = %v, shouldErr = %v", tc.input, err, tc.shouldErr)
			}
			if !tc.shouldErr && len(filters) != 1 {
				t.Fatalf("Set(%q) resulted in %d filters, want 1", tc.input, len(filters))
			}
		})
	}
}

func TestEventFiltersSetMultiple(t *testing.T) {
	var filters eventFilters

	err := filters.Set("namespace=default")
	if err != nil {
		t.Fatalf("first Set() failed: %v", err)
	}

	err = filters.Set("kind=Pod")
	if err != nil {
		t.Fatalf("second Set() failed: %v", err)
	}

	if len(filters) != 2 {
		t.Fatalf("expected 2 filters, got %d", len(filters))
	}

	if filters[0].Namespace != "default" {
		t.Errorf("first filter namespace = %q, want 'default'", filters[0].Namespace)
	}

	if filters[1].Kind != "Pod" {
		t.Errorf("second filter kind = %q, want 'Pod'", filters[1].Kind)
	}
}

func TestGetK8sConfigErrorCases(t *testing.T) {
	testCases := []struct {
		name        string
		kubeconfig  string
		shouldError bool
	}{
		{
			name:        "explicit non-existent path errors immediately",
			kubeconfig:  "/nonexistent/path/to/kubeconfig",
			shouldError: true,
		},
		{
			name:        "empty string attempts in-cluster config (errors outside a cluster)",
			kubeconfig:  "",
			shouldError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := getK8sConfig(tc.kubeconfig)
			if (err != nil) != tc.shouldError {
				t.Fatalf("getK8sConfig(%q) error = %v, shouldError = %v", tc.kubeconfig, err, tc.shouldError)
			}
		})
	}
}

func TestGetK8sConfigExplicitPathDoesNotFallBack(t *testing.T) {
	// When an explicit kubeconfig path is provided and fails, the error must
	// come from the kubeconfig loader, not from in-cluster config.
	// This ensures a bad -kubeconfig flag is always a loud failure.
	_, err := getK8sConfig("/nonexistent/path/to/kubeconfig")
	if err == nil {
		t.Fatal("expected error for non-existent kubeconfig, got nil")
	}
	// In-cluster config failure contains "KUBERNETES_SERVICE_HOST"; a
	// kubeconfig file error does not. If the fallback were still active,
	// running outside a cluster would produce an in-cluster error instead.
	if strings.Contains(err.Error(), "KUBERNETES_SERVICE_HOST") {
		t.Errorf("error looks like an in-cluster fallback error, expected kubeconfig file error: %v", err)
	}
}

func TestParseEventFilterReportingComponentVariants(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "reporting-component field",
			input:    "reporting-component=scheduler",
			expected: "scheduler",
		},
		{
			name:     "reporting-controller alias",
			input:    "reporting-controller=scheduler",
			expected: "scheduler",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filter, err := parseEventFilter(tc.input)
			if err != nil {
				t.Fatalf("parseEventFilter failed: %v", err)
			}
			if filter.ReportingComponent != tc.expected {
				t.Fatalf("ReportingComponent = %q, want %q", filter.ReportingComponent, tc.expected)
			}
		})
	}
}

func TestEventFilterComplexScenarios(t *testing.T) {
	event := &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Namespace: "production",
			Kind:      "Pod",
			Name:      "api-server-1",
		},
		Reason:              "CrashLoopBackOff",
		Type:                "Warning",
		ReportingController: "kubelet",
		Source: v1.EventSource{
			Component: "kubelet",
		},
	}

	testCases := []struct {
		name    string
		filter  eventFilter
		matches bool
	}{
		{
			name: "warn about pod in production",
			filter: eventFilter{
				Namespace: "production",
				Kind:      "Pod",
				Type:      "Warning",
			},
			matches: true,
		},
		{
			name: "only normal events in default namespace",
			filter: eventFilter{
				Namespace: "default",
				Type:      "Normal",
			},
			matches: false,
		},
		{
			name: "crash loops from kubelet",
			filter: eventFilter{
				Reason:             "CrashLoopBackOff",
				ReportingComponent: "kubelet",
			},
			matches: true,
		},
		{
			name: "specific pod monitoring",
			filter: eventFilter{
				Namespace: "production",
				Name:      "api-server-1",
			},
			matches: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.filter.Match(event)
			if got != tc.matches {
				t.Fatalf("Match() = %v, want %v", got, tc.matches)
			}
		})
	}
}

// Phase 2: Integration Testing - Event Processing & Marshaling

func TestEventWithTimeStructure(t *testing.T) {
	now := time.Now().UTC()
	event := &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Namespace: "default",
			Kind:      "Pod",
			Name:      "test-pod",
		},
		EventTime: metav1.MicroTime{Time: now},
		Type:      "Normal",
		Reason:    "Started",
	}

	// Marshal the event
	eventBytes, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	if len(eventBytes) == 0 {
		t.Fatal("marshaled event is empty")
	}

	// Verify it's valid JSON
	var unmarshaled map[string]interface{}
	err = json.Unmarshal(eventBytes, &unmarshaled)
	if err != nil {
		t.Fatalf("marshaled data is not valid JSON: %v", err)
	}
}

func TestEventMarshalingWithDifferentTypes(t *testing.T) {
	testCases := []struct {
		name      string
		eventType string
		expected  string
	}{
		{
			name:      "warning type",
			eventType: "Warning",
			expected:  "Warning",
		},
		{
			name:      "normal type",
			eventType: "Normal",
			expected:  "Normal",
		},
		{
			name:      "custom type",
			eventType: "CustomType",
			expected:  "CustomType",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := &v1.Event{
				Type: tc.eventType,
				InvolvedObject: v1.ObjectReference{
					Kind: "Pod",
				},
			}

			bytes, err := json.Marshal(event)
			if err != nil {
				t.Fatalf("failed to marshal: %v", err)
			}

			if !strings.Contains(string(bytes), tc.expected) {
				t.Errorf("marshaled event missing type %q", tc.expected)
			}
		})
	}
}

func TestEventFilteringLogic(t *testing.T) {
	excludeFilters := eventFilters{
		{Type: "Normal", Kind: "Node"},
		{Namespace: "kube-system"},
	}

	testCases := []struct {
		name      string
		event     *v1.Event
		shouldLog bool
	}{
		{
			name: "normal node event filtered",
			event: &v1.Event{
				Type: "Normal",
				InvolvedObject: v1.ObjectReference{
					Kind: "Node",
				},
			},
			shouldLog: false,
		},
		{
			name: "warning node event not filtered",
			event: &v1.Event{
				Type: "Warning",
				InvolvedObject: v1.ObjectReference{
					Kind: "Node",
				},
			},
			shouldLog: true,
		},
		{
			name: "kube-system event filtered",
			event: &v1.Event{
				InvolvedObject: v1.ObjectReference{
					Namespace: "kube-system",
					Kind:      "Pod",
				},
			},
			shouldLog: false,
		},
		{
			name: "default namespace event not filtered",
			event: &v1.Event{
				Type: "Normal",
				InvolvedObject: v1.ObjectReference{
					Namespace: "default",
					Kind:      "Pod",
				},
			},
			shouldLog: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			filtered := excludeFilters.Match(tc.event)
			shouldFilter := !tc.shouldLog
			if filtered != shouldFilter {
				t.Fatalf("Match() = %v, want %v (shouldLog=%v)", filtered, shouldFilter, tc.shouldLog)
			}
		})
	}
}

func TestEventProcessingErrorHandling(t *testing.T) {
	testCases := []struct {
		name  string
		event *v1.Event
	}{
		{
			name: "event with minimal fields",
			event: &v1.Event{
				Type: "Normal",
			},
		},
		{
			name: "event with all fields",
			event: &v1.Event{
				InvolvedObject: v1.ObjectReference{
					Namespace: "default",
					Kind:      "Pod",
					Name:      "test",
				},
				EventTime:           metav1.MicroTime{Time: time.Now()},
				LastTimestamp:       metav1.Time{Time: time.Now()},
				FirstTimestamp:      metav1.Time{Time: time.Now()},
				Type:                "Warning",
				Reason:              "TestEvent",
				ReportingController: "test-controller",
				Source: v1.EventSource{
					Component: "test-component",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test that events can be marshaled without error
			_, err := json.Marshal(tc.event)
			if err != nil {
				t.Fatalf("failed to marshal event: %v", err)
			}

			// Test timestamp extraction
			eventTime(tc.event)

			// Test level mapping
			eventLevel(tc.event.Type)

			// Test historical check
			isHistorical(tc.event, time.Now())
		})
	}
}

func TestEventBufferWriting(t *testing.T) {
	var buf bytes.Buffer

	event := &v1.Event{
		Type: "Normal",
		InvolvedObject: v1.ObjectReference{
			Namespace: "default",
			Kind:      "Pod",
			Name:      "test",
		},
	}

	// Simulate what the application does: marshal to JSON
	jsonData, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	// Write to buffer
	_, err = buf.WriteString(string(jsonData) + "\n")
	if err != nil {
		t.Fatalf("failed to write to buffer: %v", err)
	}

	if buf.Len() == 0 {
		t.Fatal("buffer is empty after write")
	}
}

func TestMultipleEventHandling(t *testing.T) {
	baseTime := time.Now().UTC()

	events := []*v1.Event{
		{
			Type:      "Normal",
			EventTime: metav1.MicroTime{Time: baseTime.Add(1 * time.Second)},
		},
		{
			Type:      "Warning",
			EventTime: metav1.MicroTime{Time: baseTime.Add(2 * time.Second)},
		},
		{
			Type:      "Normal",
			EventTime: metav1.MicroTime{Time: baseTime.Add(3 * time.Second)},
		},
	}

	for _, event := range events {
		// Each event should be processable
		level := eventLevel(event.Type)
		if level == "" {
			t.Error("eventLevel returned empty string")
		}

		// Each event should marshal correctly
		_, err := json.Marshal(event)
		if err != nil {
			t.Fatalf("failed to marshal event: %v", err)
		}

		// Historical check should work
		isHistorical(event, baseTime)
	}
}

func TestEventLogEntryFormat(t *testing.T) {
	now := time.Now().UTC()
	event := &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Namespace: "default",
			Kind:      "Pod",
			Name:      "test-pod",
		},
		Type:   "Normal",
		Reason: "Started",
	}

	entry := eventLogEntry{
		Time:  now,
		Level: "info",
		Event: event,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("Failed to marshal eventLogEntry: %v", err)
	}

	var unmarshaled map[string]interface{}
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("Failed to unmarshal eventLogEntry: %v", err)
	}

	// Verify top-level fields
	if _, ok := unmarshaled["time"]; !ok {
		t.Error("Missing 'time' field")
	}
	if unmarshaled["level"] != "info" {
		t.Errorf("level = %v, want 'info'", unmarshaled["level"])
	}

	// Verify event structure
	eventMap, ok := unmarshaled["event"].(map[string]interface{})
	if !ok {
		t.Fatal("Missing or invalid 'event' field")
	}

	involvedObject, ok := eventMap["involvedObject"].(map[string]interface{})
	if !ok {
		t.Fatal("Missing 'involvedObject' in event")
	}

	if involvedObject["name"] != "test-pod" {
		t.Errorf("involvedObject.name = %v, want 'test-pod'", involvedObject["name"])
	}
}

func TestNewAppMetricsRegistersAllMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newAppMetrics(reg, true)

	// All fields must be populated when detailed metrics are enabled.
	if m.eventsTotal == nil {
		t.Error("eventsTotal is nil")
	}
	if m.leaderGauge == nil {
		t.Error("leaderGauge is nil")
	}
	if m.leaderElectionsTotal == nil {
		t.Error("leaderElectionsTotal is nil")
	}
	if m.lastEventTimestamp == nil {
		t.Error("lastEventTimestamp is nil")
	}
	if m.eventsFilteredTotal == nil {
		t.Error("eventsFilteredTotal is nil")
	}
	if m.eventsFailedTotal == nil {
		t.Error("eventsFailedTotal is nil")
	}
	if m.eventProcessingDuration == nil {
		t.Error("eventProcessingDuration is nil")
	}
	if m.eventsByNamespaceTotal == nil {
		t.Error("eventsByNamespaceTotal is nil")
	}
	if m.eventsByReasonTotal == nil {
		t.Error("eventsByReasonTotal is nil")
	}
	if m.eventsByObjectKindTotal == nil {
		t.Error("eventsByObjectKindTotal is nil")
	}
	if m.informerCacheSyncDuration == nil {
		t.Error("informerCacheSyncDuration is nil")
	}

	// All metrics must be usable — panics here mean the metric was created
	// but not wired correctly.
	m.eventsTotal.WithLabelValues("Normal").Inc()
	m.leaderGauge.Set(1)
	m.leaderElectionsTotal.Inc()
	m.lastEventTimestamp.SetToCurrentTime()
	m.eventsFilteredTotal.WithLabelValues("historical").Inc()
	m.eventsFailedTotal.WithLabelValues("marshal_error").Inc()
	m.eventProcessingDuration.Observe(0.001)
	m.eventsByNamespaceTotal.WithLabelValues("default").Inc()
	m.eventsByReasonTotal.WithLabelValues("Scheduled").Inc()
	m.eventsByObjectKindTotal.WithLabelValues("Pod").Inc()
	m.informerCacheSyncDuration.Set(0.5)

	// All metrics must be gathered from the registry without error — confirms
	// every metric was registered and none were silently dropped.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("registry.Gather() error: %v", err)
	}
	if len(mfs) != 11 {
		t.Errorf("gathered %d metric families, want 11", len(mfs))
	}
}

func TestNewAppMetricsWithoutDetailedMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := newAppMetrics(reg, false)

	// Core metrics must be populated.
	if m.eventsTotal == nil {
		t.Error("eventsTotal is nil")
	}
	if m.leaderGauge == nil {
		t.Error("leaderGauge is nil")
	}
	if m.informerCacheSyncDuration == nil {
		t.Error("informerCacheSyncDuration is nil")
	}

	// Detailed metrics must NOT be registered.
	if m.eventsByNamespaceTotal != nil {
		t.Error("eventsByNamespaceTotal should be nil when detailed metrics are disabled")
	}
	if m.eventsByReasonTotal != nil {
		t.Error("eventsByReasonTotal should be nil when detailed metrics are disabled")
	}
	if m.eventsByObjectKindTotal != nil {
		t.Error("eventsByObjectKindTotal should be nil when detailed metrics are disabled")
	}

	// Only core metrics should be gathered (8 instead of 11).
	m.eventsTotal.WithLabelValues("Normal").Inc()
	m.leaderGauge.Set(1)
	m.leaderElectionsTotal.Inc()
	m.lastEventTimestamp.SetToCurrentTime()
	m.eventsFilteredTotal.WithLabelValues("historical").Inc()
	m.eventsFailedTotal.WithLabelValues("marshal_error").Inc()
	m.eventProcessingDuration.Observe(0.001)
	m.informerCacheSyncDuration.Set(0.5)
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("registry.Gather() error: %v", err)
	}
	if len(mfs) != 8 {
		t.Errorf("gathered %d metric families, want 8", len(mfs))
	}
}

func TestNewAppMetricsPanicsOnDuplicateRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	newAppMetrics(reg, true)
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate registration, got none")
		}
	}()
	newAppMetrics(reg, true)
}

func TestRunInvalidFlag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := run(ctx, []string{"-no-such-flag"})
	if err == nil {
		t.Fatal("expected error for unknown flag, got nil")
	}
}

func TestRunInvalidLogFormat(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := run(ctx, []string{"-log-format=unknown"})
	if err == nil {
		t.Fatal("expected error for unsupported log format, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported log format") {
		t.Fatalf("error = %q, want unsupported log format", err.Error())
	}
}

func TestRunHelpFlag(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := run(ctx, []string{"-help"})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("expected flag.ErrHelp, got %v", err)
	}
}

func TestRunBadKubeconfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := run(ctx, []string{"-kubeconfig=/nonexistent/path/to/kubeconfig"})
	if err == nil {
		t.Fatal("expected error for non-existent kubeconfig, got nil")
	}
	if strings.Contains(err.Error(), "KUBERNETES_SERVICE_HOST") {
		t.Errorf("error looks like an in-cluster fallback, want kubeconfig file error: %v", err)
	}
}

func TestRunCancelledContext(t *testing.T) {
	// kubernetes.NewForConfig only builds a struct (no network calls), so a
	// valid-format kubeconfig pointing at a fake server gets us past flag
	// parsing, config loading, clientset creation, and informer setup.
	// A pre-cancelled context causes WaitForCacheSync to return false
	// immediately, covering that whole section without needing a real cluster.
	kubeconfig := `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:1
  name: test
contexts:
- context:
    cluster: test
    user: test
  name: test
current-context: test
users:
- name: test
  user: {}
`
	f, err := os.CreateTemp(t.TempDir(), "kubeconfig-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp kubeconfig: %v", err)
	}
	if _, err := f.WriteString(kubeconfig); err != nil {
		t.Fatalf("failed to write kubeconfig: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("failed to close kubeconfig: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so WaitForCacheSync returns false immediately

	err = run(ctx, []string{"-kubeconfig=" + f.Name()})
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
	if !strings.Contains(err.Error(), "failed to wait for caches to sync") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestLeaderCallbackTrackerWaitReturnsForStandby(t *testing.T) {
	tracker := newLeaderCallbackTracker()
	done := make(chan struct{})

	go func() {
		tracker.wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("wait blocked even though leadership never started")
	}
}

func TestLeaderCallbackTrackerWaitsForLeaderCallback(t *testing.T) {
	tracker := newLeaderCallbackTracker()
	tracker.markStarted()

	done := make(chan struct{})
	go func() {
		tracker.wait()
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("wait returned before leader callback completed")
	case <-time.After(10 * time.Millisecond):
	}

	tracker.markDone()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("wait did not return after leader callback completed")
	}
}
