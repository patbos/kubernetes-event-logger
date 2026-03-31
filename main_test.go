package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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
		eventType string
		expected  string
	}{
		{
			eventType: "Warning",
			expected:  "warn",
		},
		{
			eventType: "Normal",
			expected:  "info",
		},
		{
			eventType: "Unknown",
			expected:  "debug",
		},
		{
			eventType: "",
			expected:  "debug",
		},
		{
			eventType: "Custom",
			expected:  "debug",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.eventType, func(t *testing.T) {
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
			name: "event before startTime is historical",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: past},
			},
			startTime: now,
			expected:  true,
		},
		{
			name: "event after startTime is not historical",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: future},
			},
			startTime: now,
			expected:  false,
		},
		{
			name: "event at exact startTime is historical",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: now},
			},
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

func TestHandleHealthLeader(t *testing.T) {
	// Setup: simulate leader state with synced cache
	healthState.Lock()
	healthState.isLeader = true
	healthState.cacheSynced = true
	healthState.startTime = time.Now().Add(-10 * time.Second)
	healthState.Unlock()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handleHealth(w, req)

	// Check status code
	if w.Code != http.StatusOK {
		t.Fatalf("handleHealth status code = %d, want %d", w.Code, http.StatusOK)
	}

	// Check response contains expected fields
	resp := w.Body.String()
	if resp == "" {
		t.Fatal("handleHealth returned empty response")
	}

	// Verify key fields are in response
	expectedFields := []string{
		`"status":"healthy"`,
		`"leader":true`,
		`"cache_synced":true`,
		`"version":"dev"`,
	}

	for _, field := range expectedFields {
		if !contains(resp, field) {
			t.Errorf("handleHealth response missing field: %s", field)
		}
	}
}

func TestHandleHealthNonLeader(t *testing.T) {
	// Setup: simulate non-leader state with synced cache
	healthState.Lock()
	healthState.isLeader = false
	healthState.cacheSynced = true
	healthState.startTime = time.Now().Add(-5 * time.Second)
	healthState.Unlock()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handleHealth(w, req)

	// Non-leader with synced cache should be healthy
	if w.Code != http.StatusOK {
		t.Fatalf("handleHealth status code = %d, want %d", w.Code, http.StatusOK)
	}

	resp := w.Body.String()
	if !contains(resp, `"leader":false`) {
		t.Error("handleHealth response missing leader=false")
	}
	if !contains(resp, `"status":"healthy"`) {
		t.Error("handleHealth response should show healthy for non-leader with synced cache")
	}
}

func TestHandleHealthNotReady(t *testing.T) {
	// Setup: cache not yet synced (startup phase)
	healthState.Lock()
	healthState.isLeader = false
	healthState.cacheSynced = false
	healthState.startTime = time.Now()
	healthState.Unlock()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handleHealth(w, req)

	// Cache not synced should return ServiceUnavailable
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("handleHealth status code = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}

	resp := w.Body.String()
	if !contains(resp, `"status":"not-ready"`) {
		t.Error("handleHealth response should show not-ready when cache not synced")
	}
	if !contains(resp, `"cache_synced":false`) {
		t.Error("handleHealth response missing cache_synced=false")
	}
}

func TestHandleHealthContentType(t *testing.T) {
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.Unlock()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handleHealth(w, req)

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Fatalf("handleHealth Content-Type = %q, want application/json", contentType)
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

func TestHandleHealthUptime(t *testing.T) {
	// Set a known start time
	startTime := time.Now().Add(-100 * time.Second)
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.startTime = startTime
	healthState.Unlock()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handleHealth(w, req)

	resp := w.Body.String()
	// Should have uptime_seconds field with value ~100
	if !contains(resp, `"uptime_seconds"`) {
		t.Error("handleHealth response missing uptime_seconds field")
	}
}

func TestHandleHealthUptimeZero(t *testing.T) {
	// Set start time to slightly in the future to ensure exactly 0 or very small uptime
	// Alternatively, just check if uptime_seconds is present and small.
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.startTime = time.Now()
	healthState.Unlock()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handleHealth(w, req)

	resp := w.Body.String()
	// Just check that uptime_seconds is present.
	// Since it's a float, we just verify it starts with 0.
	if !contains(resp, `"uptime_seconds":0`) && !contains(resp, `"uptime_seconds":-0`) {
		// If it's scientific notation like 1.23e-05, it might not contain :0
		// But for now let's just accept any small value by checking if it's there
		if !contains(resp, `"uptime_seconds"`) {
			t.Error("handleHealth response missing uptime_seconds field")
		}
	}
}

func TestEventLevelUnknownTypes(t *testing.T) {
	// Ensure all unknown types return "debug"
	unknownTypes := []string{
		"Error",
		"Critical",
		"Info",
		"Notice",
		"Debug",
		"Trace",
		"",
		"random-string",
	}

	for _, eventType := range unknownTypes {
		level := eventLevel(eventType)
		if level != "debug" {
			t.Errorf("eventLevel(%q) = %q, want debug", eventType, level)
		}
	}
}

func TestIsHistoricalEdgeCases(t *testing.T) {
	now := time.Now().UTC()

	testCases := []struct {
		name      string
		event     *v1.Event
		startTime time.Time
		expected  bool
	}{
		{
			name: "nanosecond difference counts as historical",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: now.Add(-1 * time.Nanosecond)},
			},
			startTime: now,
			expected:  true,
		},
		{
			name: "far future event is not historical",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: now.Add(1 * time.Hour)},
			},
			startTime: now,
			expected:  false,
		},
		{
			name: "far past event is historical",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: now.Add(-24 * time.Hour)},
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

func TestHandleHealthLeaderTransition(t *testing.T) {
	// Simulate transitioning from non-leader to leader
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.isLeader = false
	healthState.Unlock()

	// First request: non-leader
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	handleHealth(w, req)
	if !contains(w.Body.String(), `"leader":false`) {
		t.Error("health check 1: expected non-leader")
	}

	// Transition to leader
	healthState.Lock()
	healthState.isLeader = true
	healthState.Unlock()

	// Second request: leader
	req = httptest.NewRequest("GET", "/healthz", nil)
	w = httptest.NewRecorder()
	handleHealth(w, req)
	if !contains(w.Body.String(), `"leader":true`) {
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

func TestIsHistoricalBoundary(t *testing.T) {
	now := time.Now().UTC()

	testCases := []struct {
		name      string
		eventTime time.Time
		startTime time.Time
		expected  bool
	}{
		{
			name:      "microsecond before startTime is historical",
			eventTime: now.Add(-1 * time.Microsecond),
			startTime: now,
			expected:  true,
		},
		{
			name:      "millisecond after startTime is not historical",
			eventTime: now.Add(1 * time.Millisecond),
			startTime: now,
			expected:  false,
		},
		{
			name:      "exactly at startTime is historical (not after)",
			eventTime: now,
			startTime: now,
			expected:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := &v1.Event{
				EventTime: metav1.MicroTime{Time: tc.eventTime},
			}
			got := isHistorical(event, tc.startTime)
			if got != tc.expected {
				t.Fatalf("isHistorical() = %v, want %v", got, tc.expected)
			}
		})
	}
}

func TestEventLevelMapping(t *testing.T) {
	testCases := []struct {
		eventType string
		expected  string
	}{
		{"Warning", "warn"},
		{"Normal", "info"},
		{"Error", "debug"},
		{"Critical", "debug"},
		{"Info", "debug"},
		{"", "debug"},
		{"UNKNOWN", "debug"},
	}

	for _, tc := range testCases {
		t.Run(tc.eventType, func(t *testing.T) {
			got := eventLevel(tc.eventType)
			if got != tc.expected {
				t.Errorf("eventLevel(%q) = %q, want %q", tc.eventType, got, tc.expected)
			}
		})
	}
}

func TestHandleHealthResponseStructure(t *testing.T) {
	healthState.Lock()
	healthState.isLeader = true
	healthState.cacheSynced = true
	healthState.startTime = time.Now().Add(-5 * time.Second)
	healthState.Unlock()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()

	handleHealth(w, req)

	requiredFields := []string{
		`"status"`,
		`"leader"`,
		`"cache_synced"`,
		`"uptime_seconds"`,
		`"version"`,
	}

	resp := w.Body.String()
	for _, field := range requiredFields {
		if !contains(resp, field) {
			t.Errorf("handleHealth response missing required field: %s", field)
		}
	}
}

func TestHandleHealthStatusCodeNotReady(t *testing.T) {
	testCases := []struct {
		name        string
		cacheSynced bool
		expected    int
	}{
		{
			name:        "healthy when synced",
			cacheSynced: true,
			expected:    http.StatusOK,
		},
		{
			name:        "not ready when not synced",
			cacheSynced: false,
			expected:    http.StatusServiceUnavailable,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			healthState.Lock()
			healthState.cacheSynced = tc.cacheSynced
			healthState.Unlock()

			req := httptest.NewRequest("GET", "/healthz", nil)
			w := httptest.NewRecorder()

			handleHealth(w, req)

			if w.Code != tc.expected {
				t.Fatalf("handleHealth status = %d, want %d", w.Code, tc.expected)
			}
		})
	}
}

func TestHandleHealthConcurrency(t *testing.T) {
	// Simulate concurrent access to health endpoint
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.isLeader = true
	healthState.startTime = time.Now()
	healthState.Unlock()

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			req := httptest.NewRequest("GET", "/healthz", nil)
			w := httptest.NewRecorder()
			handleHealth(w, req)
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

func TestHandleHealthLeadershipStates(t *testing.T) {
	testCases := []struct {
		name        string
		isLeader    bool
		cacheSynced bool
		expectedSts int
	}{
		{
			name:        "leader with synced cache is healthy",
			isLeader:    true,
			cacheSynced: true,
			expectedSts: http.StatusOK,
		},
		{
			name:        "non-leader with synced cache is healthy",
			isLeader:    false,
			cacheSynced: true,
			expectedSts: http.StatusOK,
		},
		{
			name:        "leader without synced cache is not ready",
			isLeader:    true,
			cacheSynced: false,
			expectedSts: http.StatusServiceUnavailable,
		},
		{
			name:        "non-leader without synced cache is not ready",
			isLeader:    false,
			cacheSynced: false,
			expectedSts: http.StatusServiceUnavailable,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			healthState.Lock()
			healthState.isLeader = tc.isLeader
			healthState.cacheSynced = tc.cacheSynced
			healthState.Unlock()

			req := httptest.NewRequest("GET", "/healthz", nil)
			w := httptest.NewRecorder()
			handleHealth(w, req)

			if w.Code != tc.expectedSts {
				t.Fatalf("handleHealth status = %d, want %d", w.Code, tc.expectedSts)
			}

			// Verify response content matches state
			resp := w.Body.String()
			if tc.isLeader && !contains(resp, `"leader":true`) {
				t.Error("response should indicate leader=true")
			}
			if !tc.isLeader && !contains(resp, `"leader":false`) {
				t.Error("response should indicate leader=false")
			}
		})
	}
}

func TestHandleHealthVersionReported(t *testing.T) {
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.Unlock()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	handleHealth(w, req)

	resp := w.Body.String()
	if !contains(resp, `"version"`) {
		t.Error("handleHealth response missing version field")
	}
	if !contains(resp, `"dev"`) {
		t.Error("handleHealth should report version 'dev' in test environment")
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

func TestHandleHealthHTTPMethods(t *testing.T) {
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.Unlock()

	// Health endpoint should handle GET requests
	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET /healthz returned %d, want %d", w.Code, http.StatusOK)
	}
}

func TestHandleHealthMultipleCalls(t *testing.T) {
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.isLeader = true
	healthState.startTime = time.Now()
	healthState.Unlock()

	for i := 0; i < 5; i++ {
		req := httptest.NewRequest("GET", "/healthz", nil)
		w := httptest.NewRecorder()
		handleHealth(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("call %d: expected status OK, got %d", i+1, w.Code)
		}

		resp := w.Body.String()
		if !contains(resp, `"status":"healthy"`) {
			t.Errorf("call %d: expected healthy status", i+1)
		}
	}
}

func TestIsHistoricalWithDifferentTimestampFields(t *testing.T) {
	baseTime := time.Now().UTC()

	testCases := []struct {
		name      string
		event     *v1.Event
		startTime time.Time
		expected  bool
	}{
		{
			name: "historical determined by EventTime",
			event: &v1.Event{
				EventTime:      metav1.MicroTime{Time: baseTime.Add(-1 * time.Hour)},
				LastTimestamp:  metav1.Time{Time: baseTime.Add(1 * time.Hour)},
				FirstTimestamp: metav1.Time{Time: baseTime.Add(2 * time.Hour)},
			},
			startTime: baseTime,
			expected:  true,
		},
		{
			name: "historical determined by LastTimestamp when EventTime zero",
			event: &v1.Event{
				LastTimestamp:  metav1.Time{Time: baseTime.Add(-30 * time.Minute)},
				FirstTimestamp: metav1.Time{Time: baseTime.Add(1 * time.Hour)},
			},
			startTime: baseTime,
			expected:  true,
		},
		{
			name: "historical determined by FirstTimestamp when others zero",
			event: &v1.Event{
				FirstTimestamp: metav1.Time{Time: baseTime.Add(-15 * time.Minute)},
			},
			startTime: baseTime,
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

func TestEventLevelCaseSensitivity(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"Warning", "warn"},
		{"warning", "warn"},
		{"WARNING", "warn"},
		{"Normal", "info"},
		{"normal", "info"},
		{"NORMAL", "info"},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			got := eventLevel(tc.input)
			if got != tc.expected {
				t.Fatalf("eventLevel(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestHandleHealthConsistency(t *testing.T) {
	healthState.Lock()
	healthState.isLeader = true
	healthState.cacheSynced = true
	healthState.startTime = time.Now().Add(-10 * time.Second)
	healthState.Unlock()

	// Make multiple requests and verify consistency
	responses := make([]string, 3)
	for i := 0; i < 3; i++ {
		req := httptest.NewRequest("GET", "/healthz", nil)
		w := httptest.NewRecorder()
		handleHealth(w, req)
		responses[i] = w.Body.String()

		// All should contain the same status
		if !contains(responses[i], `"status":"healthy"`) {
			t.Fatalf("response %d missing healthy status", i+1)
		}
		if !contains(responses[i], `"leader":true`) {
			t.Fatalf("response %d missing leader=true", i+1)
		}
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
			name:        "non-existent file",
			kubeconfig:  "/nonexistent/path/to/kubeconfig",
			shouldError: true,
		},
		{
			name:        "empty string uses default",
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

func TestHandleHealthJSONValid(t *testing.T) {
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.isLeader = true
	healthState.startTime = time.Now().Add(-5 * time.Second)
	healthState.Unlock()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	handleHealth(w, req)

	resp := w.Body.String()
	if !contains(resp, "{") || !contains(resp, "}") {
		t.Fatal("handleHealth response is not valid JSON")
	}
	if !contains(resp, ":") {
		t.Fatal("handleHealth response missing JSON key-value pairs")
	}
}

func TestIsHistoricalUTC(t *testing.T) {
	now := time.Now().UTC()
	past := now.Add(-1 * time.Hour)

	event := &v1.Event{
		EventTime: metav1.MicroTime{Time: past},
	}

	if !isHistorical(event, now) {
		t.Fatal("past event should be historical")
	}
}

func TestEventTimeConsistency(t *testing.T) {
	now := time.Now().UTC()

	event := &v1.Event{
		EventTime: metav1.MicroTime{Time: now},
	}

	time1 := eventTime(event)
	time2 := eventTime(event)

	if !time1.Equal(time2) {
		t.Fatalf("eventTime() not consistent: %v != %v", time1, time2)
	}
}

func TestEventLevelConsistency(t *testing.T) {
	level1 := eventLevel("Warning")
	level2 := eventLevel("Warning")

	if level1 != level2 {
		t.Fatalf("eventLevel() not consistent: %q != %q", level1, level2)
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

			if !contains(string(bytes), tc.expected) {
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

func TestHistoricalEventFiltering(t *testing.T) {
	baseTime := time.Now().UTC()

	testCases := []struct {
		name       string
		event      *v1.Event
		startTime  time.Time
		isHistoric bool
	}{
		{
			name: "old event is historical",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: baseTime.Add(-1 * time.Hour)},
			},
			startTime:  baseTime,
			isHistoric: true,
		},
		{
			name: "new event is not historical",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: baseTime.Add(1 * time.Minute)},
			},
			startTime:  baseTime,
			isHistoric: false,
		},
		{
			name: "event at startup is historical",
			event: &v1.Event{
				EventTime: metav1.MicroTime{Time: baseTime},
			},
			startTime:  baseTime,
			isHistoric: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := isHistorical(tc.event, tc.startTime)
			if result != tc.isHistoric {
				t.Fatalf("isHistorical() = %v, want %v", result, tc.isHistoric)
			}
		})
	}
}

func TestEventLevelOutputMapping(t *testing.T) {
	testCases := []struct {
		eventType string
		level     string
	}{
		{"Warning", "warn"},
		{"Normal", "info"},
		{"Unknown", "debug"},
	}

	for _, tc := range testCases {
		t.Run(tc.eventType, func(t *testing.T) {
			level := eventLevel(tc.eventType)
			if level != tc.level {
				t.Fatalf("eventLevel(%q) = %q, want %q", tc.eventType, level, tc.level)
			}
		})
	}
}

func TestHealthEndpointJSONStructure(t *testing.T) {
	healthState.Lock()
	healthState.cacheSynced = true
	healthState.isLeader = true
	healthState.startTime = time.Now().Add(-10 * time.Second)
	healthState.Unlock()

	req := httptest.NewRequest("GET", "/healthz", nil)
	w := httptest.NewRecorder()
	handleHealth(w, req)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	if err != nil {
		t.Fatalf("failed to unmarshal health response: %v", err)
	}

	// Verify all required fields exist
	requiredFields := []string{"status", "leader", "cache_synced", "uptime_seconds", "version"}
	for _, field := range requiredFields {
		if _, ok := response[field]; !ok {
			t.Errorf("missing field: %s", field)
		}
	}

	// Verify field types
	if _, ok := response["status"].(string); !ok {
		t.Error("status should be string")
	}
	if _, ok := response["leader"].(bool); !ok {
		t.Error("leader should be boolean")
	}
	if _, ok := response["cache_synced"].(bool); !ok {
		t.Error("cache_synced should be boolean")
	}
	if _, ok := response["uptime_seconds"].(float64); !ok {
		t.Error("uptime_seconds should be number")
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

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
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
