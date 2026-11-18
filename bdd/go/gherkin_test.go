package bdd_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
)

// write puts a feature file in a temporary directory, for parser cases the
// shared scenarios do not happen to contain.
func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "case.feature")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

// A runner that forgot to run the background would test a scenario against
// state it never set up, so the background is prefixed at parse time.
func TestGivenABackgroundWhenParsedThenShouldPrefixEveryScenario(t *testing.T) {
	feature, err := bdd.ParseFeature(write(t, `
Feature: Example
  Background:
    Given a running data platform
    And a fresh stream

  Scenario: First
    When something happens
    Then it holds

  Scenario: Second
    Then it also holds
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(feature.Scenarios) != 2 {
		t.Fatalf("want two scenarios, got %d", len(feature.Scenarios))
	}
	for _, scenario := range feature.Scenarios {
		if len(scenario.Steps) < 2 {
			t.Fatalf("%s: want the background prefixed, got %d steps",
				scenario.Name, len(scenario.Steps))
		}
		if scenario.Steps[0].Text != "a running data platform" {
			t.Errorf("%s: want the background first, got %q",
				scenario.Name, scenario.Steps[0].Text)
		}
	}
	// The second scenario must not inherit the first's own steps, or a
	// scenario would pass on work another one did.
	if len(feature.Scenarios[1].Steps) != 3 {
		t.Errorf("want background plus one step, got %d",
			len(feature.Scenarios[1].Steps))
	}
}

// A failure in a Given is a broken precondition where the same text in a Then
// is a broken expectation, so the keyword is kept.
func TestGivenStepsWhenParsedThenShouldKeepTheirKeywords(t *testing.T) {
	feature, err := bdd.ParseFeature(write(t, `
Feature: Example
  Scenario: Keywords
    Given a start
    When an action
    Then a result
    And another result
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []string{"Given", "When", "Then", "And"}
	steps := feature.Scenarios[0].Steps
	if len(steps) != len(want) {
		t.Fatalf("want %d steps, got %d", len(want), len(steps))
	}
	for index, keyword := range want {
		if steps[index].Keyword != keyword {
			t.Errorf("at %d want %s, got %s", index, keyword, steps[index].Keyword)
		}
	}
}

// Tags select the subset a runner implements, so they must reach the scenario
// they sit above rather than the one after it.
func TestGivenTagsWhenParsedThenShouldAttachToWhatTheyLabel(t *testing.T) {
	feature, err := bdd.ParseFeature(write(t, `
@wire @provenance
Feature: Example
  @slow
  Scenario: Tagged
    Then it holds

  Scenario: Untagged
    Then it also holds
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(feature.Tags) != 2 {
		t.Errorf("want both feature tags, got %v", feature.Tags)
	}
	if !feature.Scenarios[0].HasTag("@slow") {
		t.Errorf("want the scenario tagged, got %v", feature.Scenarios[0].Tags)
	}
	// A tag must not leak onto the scenario after the one it labels.
	if len(feature.Scenarios[1].Tags) != 0 {
		t.Errorf("want the second scenario untagged, got %v", feature.Scenarios[1].Tags)
	}
}

// Reading only an outline's template would run a scenario with placeholders
// still in it, which would pass or fail for the wrong reason.
func TestGivenAnOutlineWhenParsedThenShouldRefuseItRatherThanHalfReadIt(t *testing.T) {
	_, err := bdd.ParseFeature(write(t, `
Feature: Example
  Scenario Outline: Templated
    Then <value> holds

  Examples:
    | value |
    | one   |
`))
	if err == nil {
		t.Fatal("an outline must be refused")
	}
	var unsupported *bdd.UnsupportedSyntaxError
	if !asUnsupported(err, &unsupported) {
		t.Fatalf("want an UnsupportedSyntaxError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "Scenario Outline") {
		t.Errorf("want the syntax named, got %v", err)
	}
}

func asUnsupported(err error, target **bdd.UnsupportedSyntaxError) bool {
	failure, ok := err.(*bdd.UnsupportedSyntaxError)
	if ok {
		*target = failure
	}
	return ok
}

// The scenarios are the shared contract, so the runner must read the real
// files rather than only the ones a test writes.
func TestGivenTheSharedScenariosWhenParsedThenShouldAllRead(t *testing.T) {
	entries, err := os.ReadDir("../scenarios")
	if err != nil {
		t.Fatalf("read the scenarios: %v", err)
	}
	read, outlines := 0, 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".feature") {
			continue
		}
		feature, err := bdd.ParseFeature(filepath.Join("../scenarios", entry.Name()))
		if err != nil {
			var unsupported *bdd.UnsupportedSyntaxError
			if asUnsupported(err, &unsupported) {
				// An outline is named rather than passed over, so the gap
				// between what the shared set holds and what this runner reads
				// stays visible.
				outlines++
				continue
			}
			t.Errorf("%s: %v", entry.Name(), err)
			continue
		}
		if len(feature.Scenarios) == 0 {
			t.Errorf("%s: parsed no scenarios", entry.Name())
		}
		read++
	}
	if read == 0 {
		t.Fatal("no shared scenarios were read, so this would pass vacuously")
	}
	t.Logf("read %d shared feature files, %d use an outline this runner does not implement",
		read, outlines)
}
