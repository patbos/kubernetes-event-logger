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
