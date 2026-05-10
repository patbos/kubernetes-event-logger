package main

import (
	"testing"

	v1 "k8s.io/api/core/v1"
)

func TestParseEventFilter(t *testing.T) {
	filter, err := parseEventFilter("namespace=kube-system,kind=Node,type=Normal,reporting-component=default-scheduler")
	if err != nil {
		t.Fatalf("parseEventFilter returned error: %v", err)
	}

	if filter.Namespace != "kube-system" {
		t.Fatalf("Namespace = %q, want kube-system", filter.Namespace)
	}
	if filter.Kind != "Node" {
		t.Fatalf("Kind = %q, want Node", filter.Kind)
	}
	if filter.Type != "Normal" {
		t.Fatalf("Type = %q, want Normal", filter.Type)
	}
	if filter.ReportingComponent != "default-scheduler" {
		t.Fatalf("ReportingComponent = %q, want default-scheduler", filter.ReportingComponent)
	}
}

func TestParseEventFilterRejectsInvalidInput(t *testing.T) {
	testCases := []string{
		"",
		"kind",
		"=Node",
		"kind=",
		"foo=bar",
	}

	for _, tc := range testCases {
		if _, err := parseEventFilter(tc); err == nil {
			t.Fatalf("parseEventFilter(%q) returned nil error", tc)
		}
	}
}

func TestEventFiltersMatch(t *testing.T) {
	event := &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Namespace: "kube-system",
			Kind:      "Node",
			Name:      "worker-1",
		},
		Reason:              "Scheduled",
		Type:                "Normal",
		ReportingController: "default-scheduler",
	}

	filters := eventFilters{
		{Kind: "Pod"},
		{Namespace: "kube-system", Kind: "Node", Type: "Normal"},
	}

	if !filters.Match(event) {
		t.Fatal("filters.Match returned false, want true")
	}
}

func TestEventFiltersDoNotMatchWhenClauseDiffers(t *testing.T) {
	event := &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Namespace: "kube-system",
			Kind:      "Node",
			Name:      "worker-1",
		},
		Reason: "NodeReady",
		Type:   "Warning",
		Source: v1.EventSource{Component: "kubelet"},
	}

	filters := eventFilters{
		{Namespace: "kube-system", Kind: "Node", Type: "Normal"},
		{ReportingComponent: "default-scheduler"},
	}

	if filters.Match(event) {
		t.Fatal("filters.Match returned true, want false")
	}
}

func TestEventReportingComponentFallbacksToSourceComponent(t *testing.T) {
	event := &v1.Event{
		Source: v1.EventSource{Component: "kubelet"},
	}

	if got := eventReportingComponent(event); got != "kubelet" {
		t.Fatalf("eventReportingComponent = %q, want kubelet", got)
	}
}

func TestParseEventFilterRejectsMalformedWildcardPattern(t *testing.T) {
	if _, err := parseEventFilter("namespace=kube-[abc"); err == nil {
		t.Fatal("parseEventFilter returned nil error for malformed pattern")
	}
}

func TestEventFilterWildcardMatch(t *testing.T) {
	testCases := []struct {
		name   string
		filter eventFilter
		event  *v1.Event
		want   bool
	}{
		{
			name:   "namespace prefix matches",
			filter: eventFilter{Namespace: "kube-*"},
			event:  &v1.Event{InvolvedObject: v1.ObjectReference{Namespace: "kube-system"}},
			want:   true,
		},
		{
			name:   "namespace prefix does not match unrelated namespace",
			filter: eventFilter{Namespace: "kube-*"},
			event:  &v1.Event{InvolvedObject: v1.ObjectReference{Namespace: "default"}},
			want:   false,
		},
		{
			name:   "reason wildcard matches BackOff variants",
			filter: eventFilter{Reason: "BackOff*"},
			event:  &v1.Event{Reason: "BackOffStart"},
			want:   true,
		},
		{
			name:   "wildcard does not match across slashes",
			filter: eventFilter{Name: "frontend-*"},
			event:  &v1.Event{InvolvedObject: v1.ObjectReference{Name: "frontend-team/api"}},
			want:   false,
		},
		{
			name:   "exact match still works without wildcard",
			filter: eventFilter{Type: "Normal"},
			event:  &v1.Event{Type: "Normal"},
			want:   true,
		},
		{
			name: "mixed wildcard and exact clauses match",
			filter: eventFilter{
				Namespace: "kube-*",
				Kind:      "Pod",
				Reason:    "BackOff*",
			},
			event: &v1.Event{
				InvolvedObject: v1.ObjectReference{Namespace: "kube-system", Kind: "Pod"},
				Reason:         "BackOffStart",
			},
			want: true,
		},
		{
			name: "mixed wildcard and exact clauses with one mismatch",
			filter: eventFilter{
				Namespace: "kube-*",
				Kind:      "Pod",
				Reason:    "BackOff*",
			},
			event: &v1.Event{
				InvolvedObject: v1.ObjectReference{Namespace: "kube-system", Kind: "Node"},
				Reason:         "BackOffStart",
			},
			want: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.filter.Match(tc.event); got != tc.want {
				t.Fatalf("Match = %v, want %v", got, tc.want)
			}
		})
	}
}
