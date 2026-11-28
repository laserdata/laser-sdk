package bdd_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime/debug"
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
	address := deploymentAddress()
	if address == "" {
		t.Skip("set LASER_CONNECTION_STRING (or run under " +
			"scripts/run-with-test-iggy.mjs, which sets it) to run the " +
			"conformance scenarios against a deployment")
	}
	return address
}

// deploymentAddress is the address the scenarios run against, empty when
// none is configured. Unlike deployment, it never skips: a mostly-local
// feature file (runLocalFeature) has no deployment for most of its
// scenarios, but a handful of @managed ones still need "a running data
// platform" to connect with, and a hardcoded empty address broke exactly
// those -- found by the first run of this suite against a real managed
// deployment, where materialization.feature's three managed scenarios all
// failed with "connection string missing credentials" despite a real,
// working deployment being configured.
func deploymentAddress() string {
	// The same precedence scripts/run-with-test-iggy.mjs reads, so a run
	// driven by that helper reaches the server it actually started. The
	// helper exports LASER_CONNECTION_STRING and LASER_BDD_URL for the
	// child, and treats LASER_BDD_ADDR as a bare host:port it prepends
	// credentials to -- so reading LASER_BDD_ADDR first, as this did, took
	// the one value of the three that carries no credentials and failed
	// against a server the helper had started correctly.
	for _, name := range []string{
		"LASER_CONNECTION_STRING", "LASER_BDD_URL", "LASER_BDD_ADDR",
	} {
		address := os.Getenv(name)
		if address == "" {
			continue
		}
		// A bare host:port gets the same default credentials the helper
		// would have given it. An address that already names a user is
		// left exactly as written.
		if !strings.Contains(address, "@") {
			address = "iggy:iggy@" + address
		}
		return address
	}
	return ""
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
	address := deployment(t)
	feature, err := bdd.ParseFeature(filepath.Join(scenarios, file))
	if err != nil {
		t.Fatalf("read the scenarios: %v", err)
	}
	if len(feature.Scenarios) == 0 {
		t.Fatalf("%s holds no scenarios, so this would pass vacuously", file)
	}
	runScenarios(t, feature, address, timeout, run)
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
	// Most scenarios in a "local" feature need no deployment at all, but a
	// handful may still be @managed and carry their own "Given a running
	// data platform" step -- deploymentAddress (unlike deployment) reports
	// empty rather than skipping the whole file when none is configured, so
	// those scenarios connect when a deployment is there and fail openly,
	// not cryptically, when it is not.
	runScenarios(t, feature, deploymentAddress(), timeout, run)
}

// featureHasTag reports whether the feature itself carries a tag, which
// every scenario in it inherits.
func featureHasTag(feature *bdd.Feature, tag string) bool {
	for _, carried := range feature.Tags {
		if carried == tag {
			return true
		}
	}
	return false
}

func runScenarios(
	t *testing.T, feature *bdd.Feature, address string,
	timeout time.Duration, run stepFunc,
) {
	t.Helper()
	for _, scenario := range feature.Scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			// A tag on the Feature covers every scenario in it, which is
			// what Gherkin means by a feature-level tag. Reading only the
			// scenario's own tags let a whole feature marked @managed run
			// against a deployment that serves none, where it failed on the
			// client's own "needs a managed backend" refusal rather than
			// skipping.
			tagged := func(tag string) bool {
				return scenario.HasTag(tag) || featureHasTag(feature, tag)
			}
			if recoveringCatalog() || tagged("@recovering_catalog") {
				if !recoveringCatalog() {
					t.Skip("needs a catalog that refuses the first three " +
						"binding reads: run with --recovering-catalog")
				}
				if !tagged("@recovering_catalog") {
					t.Skip("a refusing catalog refuses this scenario too")
				}
			}
			if tagged("@unmanaged") && managed() {
				t.Skip("requires a deployment without a managed backend")
			}
			if tagged("@remote_sql") && os.Getenv("LASER_BDD_SQL_BACKEND") == "" {
				t.Skip("requires a declared SQL backend")
			}
			if tagged("@managed") && !managed() {
				t.Skip("needs a managed backend: set LASER_BDD_MANAGED=1")
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			state := &world{t: t, address: address}
			for _, step := range scenario.Steps {
				err := runStep(ctx, state, step, run)
				if err != nil {
					t.Fatalf("%s %s: %v", step.Keyword, step.Text, err)
				}
			}
		})
	}
}

// runStep reports a binding that panics, on a reworded step for one, as that
// step's error with its stack.
func runStep(ctx context.Context, state *world, step bdd.Step, run stepFunc) (err error) {
	defer func() {
		if failure := recover(); failure != nil {
			err = fmt.Errorf("the step binding panicked: %v\n%s", failure, debug.Stack())
		}
	}()
	return run(ctx, state, step)
}

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
		return true, state.bootstrapForSessions(ctx, 1)

	case strings.HasPrefix(text, "a fresh stream bootstrapped with "):
		counts := numbersOf(step)
		if len(counts) != 1 {
			return true, fmt.Errorf("want one partition count, got %v", counts)
		}
		if err := state.freshStream(ctx); err != nil {
			return true, err
		}
		return true, state.bootstrapForSessions(ctx, uint32(counts[0]))
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

// bootstrapForSessions creates the stream's agent topics and, on a
// deployment that serves sessions, also registers the stream as a session
// source.
//
// Sessions.Bootstrap is a strict superset of the plain Laser.Bootstrap this
// used to call directly: the registration step is a no-op (Registered:
// false, never an error) on a deployment with no session capability, so
// calling it unconditionally is safe for every scenario that uses this
// shared step, not only the ones that read a session back afterward. Found
// by the first run of this suite against a real managed deployment: every
// scenario that reads a session through the managed index (list/get/
// events/state/links/sources/changes/watch) failed with "stream is not
// registered for sessions" because this step never registered anything.
func (state *world) bootstrapForSessions(ctx context.Context, partitions uint32) error {
	_, err := state.client.Sessions().Bootstrap(ctx, partitions, laser.ExpireAfter(24*time.Hour))
	return err
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
