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
	"regexp"
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

// placeholder is an outline parameter as it appears in a templated step.
var placeholder = regexp.MustCompile(`<([A-Za-z_][A-Za-z0-9_]*)>`)

// outline is a scenario template waiting for the example rows that fill it.
type outline struct {
	name    string
	tags    []string
	steps   []Step
	columns []string
	// rows is every example row gathered so far. An outline may carry more
	// than one Examples block, and each row becomes its own scenario.
	rows [][]string
	// reading says whether the parser is inside an Examples table, so a row is
	// only read where one belongs rather than anywhere a pipe appears.
	reading bool
}

// ParseFeature reads a Gherkin file.
//
// It handles the subset the shared scenarios use: tags, a feature, a
// background, scenarios, scenario outlines with their examples, and steps. A
// row of an outline becomes its own scenario with the parameters already
// filled in, so a runner never sees a placeholder and never has to expand one
// itself.
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
	var template *outline
	inBackground := false

	// flush turns the gathered template into one scenario per example row. It
	// runs whenever a new block starts, so the rows of the block that just
	// ended are never attributed to the next one.
	flush := func() error {
		if template == nil {
			return nil
		}
		expanded, err := template.expand(path)
		if err != nil {
			return err
		}
		feature.Scenarios = append(feature.Scenarios, expanded...)
		template = nil
		return nil
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case line == "" || strings.HasPrefix(line, "#"):
			continue

		case strings.HasPrefix(line, "|"):
			if template != nil && template.reading {
				template.row(cells(line))
			}

		case strings.HasPrefix(line, "@"):
			pending = append(pending, strings.Fields(line)...)

		case strings.HasPrefix(line, "Feature:"):
			if err := flush(); err != nil {
				return nil, err
			}
			feature.Name = strings.TrimSpace(strings.TrimPrefix(line, "Feature:"))
			feature.Tags, pending = pending, nil

		case strings.HasPrefix(line, "Background:"):
			if err := flush(); err != nil {
				return nil, err
			}
			inBackground = true
			current = nil

		case strings.HasPrefix(line, "Scenario Outline:"):
			if err := flush(); err != nil {
				return nil, err
			}
			inBackground = false
			current = nil
			template = &outline{
				name:  strings.TrimSpace(strings.TrimPrefix(line, "Scenario Outline:")),
				tags:  pending,
				steps: append([]Step(nil), background...),
			}
			pending = nil

		case strings.HasPrefix(line, "Examples:"):
			if template == nil {
				return nil, &UnsupportedSyntaxError{Path: path, Line: line}
			}
			// A second Examples block restarts the header, so its columns are
			// read again rather than inherited from the block before it.
			template.columns = nil
			template.reading = true
			pending = nil

		case strings.HasPrefix(line, "Scenario:"):
			if err := flush(); err != nil {
				return nil, err
			}
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
				switch {
				case inBackground:
					background = append(background, step)
				case template != nil:
					template.steps = append(template.steps, step)
				case current != nil:
					current.Steps = append(current.Steps, step)
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return feature, nil
}

// cells splits one table line into its trimmed values.
func cells(line string) []string {
	trimmed := strings.Trim(strings.TrimSpace(line), "|")
	parts := strings.Split(trimmed, "|")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		values = append(values, strings.TrimSpace(part))
	}
	return values
}

// row takes the next line of an Examples table: the first one is the header.
func (template *outline) row(values []string) {
	if template.columns == nil {
		template.columns = values
		return
	}
	template.rows = append(template.rows, values)
}

// expand turns the template into one scenario per example row.
//
// A row whose width does not match the header, or a step left holding a
// placeholder no column fills, is refused rather than run: either would make a
// scenario pass or fail for a reason the shared contract never stated.
func (template *outline) expand(path string) ([]Scenario, error) {
	if len(template.rows) == 0 {
		return nil, &UnsupportedSyntaxError{
			Path: path,
			Line: "Scenario Outline: " + template.name + " carries no examples",
		}
	}
	scenarios := make([]Scenario, 0, len(template.rows))
	for _, row := range template.rows {
		if len(row) != len(template.columns) {
			return nil, &UnsupportedSyntaxError{
				Path: path,
				Line: "Examples: a row of " + template.name +
					" does not match its header",
			}
		}
		values := make(map[string]string, len(row))
		for index, column := range template.columns {
			values[column] = row[index]
		}
		steps := make([]Step, 0, len(template.steps))
		for _, step := range template.steps {
			filled := fill(step.Text, values)
			if left := placeholder.FindString(filled); left != "" {
				return nil, &UnsupportedSyntaxError{
					Path: path,
					Line: "Scenario Outline: " + template.name +
						" leaves " + left + " unfilled",
				}
			}
			steps = append(steps, Step{Keyword: step.Keyword, Text: filled})
		}
		scenarios = append(scenarios, Scenario{
			// The row values are part of the name, so two rows of one outline
			// never report under the same scenario, which would hide which of
			// them failed.
			Name:  fill(template.name, values) + " [" + strings.Join(row, ", ") + "]",
			Tags:  append([]string(nil), template.tags...),
			Steps: steps,
		})
	}
	return scenarios, nil
}

// fill substitutes the example row's values into a templated line.
func fill(text string, values map[string]string) string {
	return placeholder.ReplaceAllStringFunc(text, func(match string) string {
		name := match[1 : len(match)-1]
		if value, found := values[name]; found {
			return value
		}
		return match
	})
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
