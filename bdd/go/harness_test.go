package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The scenarios live beside the other runners' so every client is held to one
// set rather than each keeping its own.
const scenarios = "../scenarios"

// deployment is the address the scenarios run against.
//
// Without one the suite skips rather than passing: a conformance run that
// reported success without reaching a server would be worse than no run, since
// it would be read as evidence.
func deployment(t *testing.T) string {
	t.Helper()
	address := os.Getenv("LASER_BDD_ADDR")
	if address == "" {
		address = os.Getenv("LASER_CONNECTION_STRING")
	}
	if address == "" {
		t.Skip("set LASER_BDD_ADDR or LASER_CONNECTION_STRING to run the " +
			"conformance scenarios against a deployment")
	}
	return address
}

// managed reports whether the scenarios tagged @managed may run.
//
// They need a server that folds the managed surfaces, which open Apache Iggy
// does not, so they stay out unless the operator says one is there.
func managed() bool {
	return os.Getenv("LASER_BDD_MANAGED") == "1"
}

// recoveringCatalog reports whether the scenarios tagged @recovering_catalog
// may run.
//
// They need a catalog that refuses the first three binding reads, which
// `scripts/run-bdd-tests.sh <language> --recovering-catalog` stands up. That
// catalog refuses every other scenario too, so the run is one or the other
// rather than both, as the Rust, Python and TypeScript runners also decide.
func recoveringCatalog() bool {
	return os.Getenv("LASER_BDD_RECOVERING_CATALOG") != ""
}

// world is the state one scenario builds up as its steps run.
//
// One struct serves every feature rather than one per runner, because the
// shared background steps ("a running data platform", "a fresh stream") are
// the same everywhere and a scenario reads better when its own fields sit
// beside them.
type world struct {
	t       *testing.T
	address string

	client       *laser.Laser
	stream       string
	conversation wire.ConversationID

	// assembled is what the last context assembly read back.
	assembled []laser.ContextMessage
	// failure is the error the last acting step returned, for the Then steps
	// that assert on how a call failed rather than that it succeeded.
	failure error
	// values carries whatever one feature's steps need to hand each other.
	// A typed field per feature would crowd this struct with names no other
	// feature reads.
	values map[string]any
}

// put and get pass a value between the steps of one scenario.
func (state *world) put(key string, value any) {
	if state.values == nil {
		state.values = map[string]any{}
	}
	state.values[key] = value
}

func (state *world) get(key string) (any, bool) {
	value, found := state.values[key]
	return value, found
}

// stepFunc runs one step, returning an error the runner reports against the
// step's own text.
type stepFunc func(ctx context.Context, state *world, step bdd.Step) error

// runFeature runs every scenario of one shared feature file.
//
// A scenario gets a fresh world, so nothing one scenario published or left
// open can make the next one pass.
func runFeature(t *testing.T, file string, timeout time.Duration, run stepFunc) {
	t.Helper()
	runFeaturePlanned(t, file, timeout, run, nil)
}

// runFeaturePlanned runs a feature where some scenarios are knowingly not
// covered yet.
//
// plan maps a scenario's name to why this runner does not run it, and those
// scenarios report as skips naming that reason. It exists so a large feature
// can be covered in slices without the uncovered ones either failing the gate
// or, far worse, quietly reporting a pass: a scenario absent from the plan and
// missing a step implementation still fails. Shrinking the plan to nothing is
// the work.
func runFeaturePlanned(
	t *testing.T, file string, timeout time.Duration,
	run stepFunc, plan map[string]string,
) {
	t.Helper()
	address := deployment(t)
	feature, err := bdd.ParseFeature(filepath.Join(scenarios, file))
	if err != nil {
		t.Fatalf("read the scenarios: %v", err)
	}
	if len(feature.Scenarios) == 0 {
		t.Fatalf("%s holds no scenarios, so this would pass vacuously", file)
	}
	// A plan entry naming a scenario this feature does not hold is stale, and
	// would go on excusing a scenario that no longer exists.
	named := map[string]bool{}
	for _, scenario := range feature.Scenarios {
		named[scenario.Name] = true
	}
	for name := range plan {
		if !named[name] {
			t.Fatalf("%s holds no scenario %q, so the plan entry is stale",
				file, name)
		}
	}
	runPlanned(t, feature, address, timeout, run, plan)
}

// runLocalFeature runs a feature whose steps reach no deployment.
//
// Those scenarios are in-process wire and evaluator rules, so making them skip
// for want of a server would hide a real regression behind an unrelated
// absence.
func runLocalFeature(t *testing.T, file string, timeout time.Duration, run stepFunc) {
	t.Helper()
	feature, err := bdd.ParseFeature(filepath.Join(scenarios, file))
	if err != nil {
		t.Fatalf("read the scenarios: %v", err)
	}
	if len(feature.Scenarios) == 0 {
		t.Fatalf("%s holds no scenarios, so this would pass vacuously", file)
	}
	runScenarios(t, feature, "", timeout, run)
}

func runScenarios(
	t *testing.T, feature *bdd.Feature, address string,
	timeout time.Duration, run stepFunc,
) {
	t.Helper()
	runPlanned(t, feature, address, timeout, run, nil)
}

func runPlanned(
	t *testing.T, feature *bdd.Feature, address string,
	timeout time.Duration, run stepFunc, plan map[string]string,
) {
	t.Helper()
	for _, scenario := range feature.Scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			if recoveringCatalog() || scenario.HasTag("@recovering_catalog") {
				if !recoveringCatalog() {
					t.Skip("needs a catalog that refuses the first three " +
						"binding reads: run with --recovering-catalog")
				}
				if !scenario.HasTag("@recovering_catalog") {
					t.Skip("a refusing catalog refuses this scenario too")
				}
			}
			if scenario.HasTag("@managed") && !managed() {
				t.Skip("needs a managed backend: set LASER_BDD_MANAGED=1")
			}
			if reason, planned := plan[scenario.Name]; planned {
				t.Skip(reason)
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			state := &world{t: t, address: address}
			for _, step := range scenario.Steps {
				err := run(ctx, state, step)
				var missing *blockedError
				if errors.As(err, &missing) {
					t.Skipf("%s %s: %v", step.Keyword, step.Text, err)
				}
				if err != nil {
					t.Fatalf("%s %s: %v", step.Keyword, step.Text, err)
				}
			}
		})
	}
}

// blockedError says a step cannot be honoured by the Go client as it stands.
//
// It is reported as a skip naming what is missing rather than as a pass, so
// the gap stays countable: a scenario quietly dropped from the runner would
// read as one the client satisfies. The steps around it stay implemented, so
// the scenario runs for real the moment the client closes the gap.
type blockedError struct{ Reason string }

func (e *blockedError) Error() string { return e.Reason }

func blocked(reason string) error { return &blockedError{Reason: reason} }

// quoted pulls the quoted arguments out of a step, which is how the shared
// scenarios carry their values.
var quoted = regexp.MustCompile(`"([^"]*)"`)

func argumentsOf(step bdd.Step) []string {
	var found []string
	for _, match := range quoted.FindAllStringSubmatch(step.Text, -1) {
		found = append(found, match[1])
	}
	return found
}

// numbers pulls the bare integers out of a step, for the counts the scenarios
// write unquoted.
var numbers = regexp.MustCompile(`\b(\d+)\b`)

func numbersOf(step bdd.Step) []uint64 {
	var found []uint64
	for _, match := range numbers.FindAllStringSubmatch(step.Text, -1) {
		value, err := strconv.ParseUint(match[1], 10, 64)
		if err != nil {
			continue
		}
		found = append(found, value)
	}
	return found
}

// list reads a comma-separated step argument, such as "a1, a2".
func list(value string) []string {
	var found []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			found = append(found, part)
		}
	}
	return found
}

// background runs the steps every feature shares, reporting whether it
// recognised the step so a feature's own runner can take the rest.
func background(ctx context.Context, state *world, step bdd.Step) (bool, error) {
	text := step.Text
	switch {
	case text == "a running data platform":
		return true, state.connect(ctx)

	case text == "a new conversation":
		state.conversation = laser.NewConversationID()
		return true, nil

	case text == "a fresh stream":
		return true, state.freshStream(ctx)

	case strings.HasPrefix(text, "a fresh stream bootstrapped with the defaults"):
		if err := state.freshStream(ctx); err != nil {
			return true, err
		}
		return true, state.client.Bootstrap(ctx, 1, laser.ExpireAfter(24*time.Hour))

	case strings.HasPrefix(text, "a fresh stream bootstrapped with "):
		counts := numbersOf(step)
		if len(counts) != 1 {
			return true, fmt.Errorf("want one partition count, got %v", counts)
		}
		if err := state.freshStream(ctx); err != nil {
			return true, err
		}
		return true, state.client.Bootstrap(
			ctx, uint32(counts[0]), laser.ExpireAfter(24*time.Hour))
	}
	return false, nil
}

// connect opens the client the scenario runs against.
//
// Closing is registered first so it runs last: a cleanup that deletes a stream
// is registered later and needs the connection still open.
func (state *world) connect(ctx context.Context) error {
	client, err := laser.Connect(ctx, state.address)
	if err != nil {
		return err
	}
	state.client = client
	state.t.Cleanup(func() { _ = client.Close() })
	return nil
}

// freshStream names a stream for this run alone and removes it afterwards.
//
// Each run names its own, so repeated runs never collide, and each cleans up
// after itself rather than leaving the deployment to fill.
func (state *world) freshStream(ctx context.Context) error {
	if state.client == nil {
		return fmt.Errorf("no client: the background never connected")
	}
	state.stream = fmt.Sprintf("bdd-go-%d", time.Now().UnixNano())
	state.client = state.client.WithDefaultStream(state.stream)
	stream := state.client.Stream(state.stream)
	if err := stream.Ensure(ctx); err != nil {
		return err
	}
	state.t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := stream.Delete(cleanup); err != nil {
			state.t.Logf("could not delete %s: %v", state.stream, err)
		}
	})
	return nil
}

// topic reaches a topic of the scenario's own stream.
func (state *world) topic(name string) *laser.Topic {
	return state.client.Stream(state.stream).Topic(name)
}

// replay reads every record a topic holds from offset zero.
func (state *world) replay(ctx context.Context, name string) ([]*laser.Record, error) {
	cursor, err := state.topic(name).Replay()
	if err != nil {
		return nil, err
	}
	return cursor.Poll(ctx)
}
