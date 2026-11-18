// Package bdd runs the shared cross-SDK conformance scenarios against the Go
// client.
//
// The scenarios under bdd/scenarios are the contract every client is held to,
// so they are read from there rather than restated here: a runner with its own
// copy would drift from the others and stop being a shared test.
package bdd

import (
	"bufio"
	"os"
	"strings"
)

// Step is one Given, When or Then of a scenario.
//
// The keyword is kept rather than normalised away, because a step reads
// differently depending on it: a failure in a Given is a broken precondition
// where the same text in a Then is a broken expectation.
type Step struct {
	Keyword string
	Text    string
}

// Scenario is one named case, with the background steps already prefixed.
//
// Prefixing at parse time is what keeps a runner from having to remember to
// run the background: forgetting would make a scenario pass against state it
// never set up.
type Scenario struct {
	Name  string
	Tags  []string
	Steps []Step
}

// Feature is one scenario file.
type Feature struct {
	Name      string
	Tags      []string
	Scenarios []Scenario
}

// ParseFeature reads a Gherkin file.
//
// It handles the subset the shared scenarios use: tags, a feature, a
// background, scenarios and their steps. An outline or a table would be
// silently half-read, so one is refused rather than parsed wrongly.
func ParseFeature(path string) (*Feature, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	feature := &Feature{}
	var pending []string
	var background []Step
	var current *Scenario
	inBackground := false

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case line == "" || strings.HasPrefix(line, "#"):
			continue

		case strings.HasPrefix(line, "@"):
			pending = append(pending, strings.Fields(line)...)

		case strings.HasPrefix(line, "Feature:"):
			feature.Name = strings.TrimSpace(strings.TrimPrefix(line, "Feature:"))
			feature.Tags, pending = pending, nil

		case strings.HasPrefix(line, "Background:"):
			inBackground = true
			current = nil

		case strings.HasPrefix(line, "Scenario Outline:"), strings.HasPrefix(line, "Examples:"):
			// An outline expands into several scenarios through its examples
			// table. Reading only the template would run a scenario with
			// placeholders still in it, which would pass or fail for the wrong
			// reason, so it is refused until the runner supports it.
			return nil, &UnsupportedSyntaxError{Path: path, Line: line}

		case strings.HasPrefix(line, "Scenario:"):
			inBackground = false
			feature.Scenarios = append(feature.Scenarios, Scenario{
				Name:  strings.TrimSpace(strings.TrimPrefix(line, "Scenario:")),
				Tags:  pending,
				Steps: append([]Step(nil), background...),
			})
			pending = nil
			current = &feature.Scenarios[len(feature.Scenarios)-1]

		default:
			keyword, text, found := strings.Cut(line, " ")
			if !found {
				continue
			}
			switch keyword {
			case "Given", "When", "Then", "And", "But":
				step := Step{Keyword: keyword, Text: strings.TrimSpace(text)}
				if inBackground {
					background = append(background, step)
					continue
				}
				if current != nil {
					current.Steps = append(current.Steps, step)
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return feature, nil
}

// UnsupportedSyntaxError reports Gherkin this runner does not implement.
//
// It is an error rather than a skip, because a half-read scenario would report
// a result it did not earn.
type UnsupportedSyntaxError struct {
	Path string
	Line string
}

func (e *UnsupportedSyntaxError) Error() string {
	return e.Path + ": this runner does not implement " + e.Line
}

// HasTag reports whether a scenario carries a tag, for a runner selecting the
// subset it implements.
func (scenario Scenario) HasTag(tag string) bool {
	for _, carried := range scenario.Tags {
		if carried == tag {
			return true
		}
	}
	return false
}
