// Package main provides event filter parsing and matching for the Kubernetes event logger.
package main

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
)

type eventFilter struct {
	Namespace          string
	Kind               string
	Name               string
	Reason             string
	Type               string
	ReportingComponent string
	SourceComponent    string
}

type eventFilters []eventFilter

func (f *eventFilters) String() string {
	rules := make([]string, 0, len(*f))
	for _, rule := range *f {
		rules = append(rules, rule.String())
	}
	return strings.Join(rules, ";")
}

func (f *eventFilters) Set(value string) error {
	rule, err := parseEventFilter(value)
	if err != nil {
		return err
	}
	*f = append(*f, rule)
	return nil
}

func (f eventFilters) Match(event *v1.Event) bool {
	for _, rule := range f {
		if rule.Match(event) {
			return true
		}
	}
	return false
}

func (f eventFilter) Match(event *v1.Event) bool {
	if f.Namespace != "" && event.InvolvedObject.Namespace != f.Namespace {
		return false
	}
	if f.Kind != "" && event.InvolvedObject.Kind != f.Kind {
		return false
	}
	if f.Name != "" && event.InvolvedObject.Name != f.Name {
		return false
	}
	if f.Reason != "" && event.Reason != f.Reason {
		return false
	}
	if f.Type != "" && event.Type != f.Type {
		return false
	}
	if f.ReportingComponent != "" && eventReportingComponent(event) != f.ReportingComponent {
		return false
	}
	if f.SourceComponent != "" && event.Source.Component != f.SourceComponent {
		return false
	}
	return true
}

func (f eventFilter) String() string {
	parts := make([]string, 0, 7)
	if f.Namespace != "" {
		parts = append(parts, "namespace="+f.Namespace)
	}
	if f.Kind != "" {
		parts = append(parts, "kind="+f.Kind)
	}
	if f.Name != "" {
		parts = append(parts, "name="+f.Name)
	}
	if f.Reason != "" {
		parts = append(parts, "reason="+f.Reason)
	}
	if f.Type != "" {
		parts = append(parts, "type="+f.Type)
	}
	if f.ReportingComponent != "" {
		parts = append(parts, "reporting-component="+f.ReportingComponent)
	}
	if f.SourceComponent != "" {
		parts = append(parts, "source-component="+f.SourceComponent)
	}
	return strings.Join(parts, ",")
}

func parseEventFilter(input string) (eventFilter, error) {
	var filter eventFilter

	for _, clause := range strings.Split(input, ",") {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}

		key, value, ok := strings.Cut(clause, "=")
		if !ok {
			return eventFilter{}, fmt.Errorf("invalid filter clause %q: expected key=value", clause)
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			return eventFilter{}, fmt.Errorf("invalid filter clause %q: key and value must be non-empty", clause)
		}

		switch key {
		case "namespace":
			filter.Namespace = value
		case "kind":
			filter.Kind = value
		case "name":
			filter.Name = value
		case "reason":
			filter.Reason = value
		case "type":
			filter.Type = value
		case "reporting-component", "reporting-controller":
			filter.ReportingComponent = value
		case "source-component":
			filter.SourceComponent = value
		default:
			return eventFilter{}, fmt.Errorf("unsupported filter field %q", key)
		}
	}

	if filter == (eventFilter{}) {
		return eventFilter{}, fmt.Errorf("filter %q did not contain any clauses", input)
	}

	return filter, nil
}

func eventReportingComponent(event *v1.Event) string {
	if event.ReportingController != "" {
		return event.ReportingController
	}
	return event.Source.Component
}
