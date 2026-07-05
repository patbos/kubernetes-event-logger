// Package main provides event filter parsing and matching for the Kubernetes event logger.
package main

import (
	"fmt"
	"path"
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

// filterField describes a single filterable event field: its flag key
// (plus optional aliases), where the pattern lives on eventFilter, and how
// to read the corresponding value from an event. Match, String, and
// parseEventFilter all iterate this table so a new field only needs to be
// added here (and to the eventFilter struct).
type filterField struct {
	key        string
	aliases    []string
	pattern    func(f *eventFilter) *string
	eventValue func(e *v1.Event) string
}

var filterFields = []filterField{
	{
		key:        "namespace",
		pattern:    func(f *eventFilter) *string { return &f.Namespace },
		eventValue: func(e *v1.Event) string { return e.InvolvedObject.Namespace },
	},
	{
		key:        "kind",
		pattern:    func(f *eventFilter) *string { return &f.Kind },
		eventValue: func(e *v1.Event) string { return e.InvolvedObject.Kind },
	},
	{
		key:        "name",
		pattern:    func(f *eventFilter) *string { return &f.Name },
		eventValue: func(e *v1.Event) string { return e.InvolvedObject.Name },
	},
	{
		key:        "reason",
		pattern:    func(f *eventFilter) *string { return &f.Reason },
		eventValue: func(e *v1.Event) string { return e.Reason },
	},
	{
		key:        "type",
		pattern:    func(f *eventFilter) *string { return &f.Type },
		eventValue: func(e *v1.Event) string { return e.Type },
	},
	{
		key:        "reporting-component",
		aliases:    []string{"reporting-controller"},
		pattern:    func(f *eventFilter) *string { return &f.ReportingComponent },
		eventValue: eventReportingComponent,
	},
	{
		key:        "source-component",
		pattern:    func(f *eventFilter) *string { return &f.SourceComponent },
		eventValue: func(e *v1.Event) string { return e.Source.Component },
	},
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
	for _, field := range filterFields {
		if !matchClause(*field.pattern(&f), field.eventValue(event)) {
			return false
		}
	}
	return true
}

// matchClause returns true if pattern is empty (clause not configured) or
// pattern matches value. When pattern contains shell-style wildcard syntax
// ("*", "?", or character classes via "["), path.Match is used; otherwise the
// comparison is exact. Patterns are validated at parse time so a Match-time
// path.Match error is treated as a non-match defensively.
func matchClause(pattern, value string) bool {
	if pattern == "" {
		return true
	}
	if !hasWildcard(pattern) {
		return pattern == value
	}
	ok, err := path.Match(pattern, value)
	if err != nil {
		return false
	}
	return ok
}

func hasWildcard(s string) bool {
	return strings.ContainsAny(s, "*?[")
}

func (f eventFilter) String() string {
	parts := make([]string, 0, len(filterFields))
	for _, field := range filterFields {
		if v := *field.pattern(&f); v != "" {
			parts = append(parts, field.key+"="+v)
		}
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

		if hasWildcard(value) {
			if _, err := path.Match(value, ""); err != nil {
				return eventFilter{}, fmt.Errorf("invalid filter clause %q: malformed pattern: %w", clause, err)
			}
		}

		field := filterFieldByKey(key)
		if field == nil {
			return eventFilter{}, fmt.Errorf("unsupported filter field %q", key)
		}
		*field.pattern(&filter) = value
	}

	if filter == (eventFilter{}) {
		return eventFilter{}, fmt.Errorf("filter %q did not contain any clauses", input)
	}

	return filter, nil
}

func filterFieldByKey(key string) *filterField {
	for i := range filterFields {
		field := &filterFields[i]
		if field.key == key {
			return field
		}
		for _, alias := range field.aliases {
			if alias == key {
				return field
			}
		}
	}
	return nil
}

func eventReportingComponent(event *v1.Event) string {
	if event.ReportingController != "" {
		return event.ReportingController
	}
	return event.Source.Component
}
