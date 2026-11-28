// Package examples holds the pieces the Go examples share.
//
// Each example is a standalone program under its own directory, run against a
// deployment named by LASER_CONNECTION_STRING. They are written to be read as
// much as run, so each one does one thing and says why.
package examples

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Feature is a deployment feature an example can require before it runs.
type Feature string

const (
	FeatureManaged        Feature = "managed"
	FeatureQuery          Feature = "query"
	FeatureKv             Feature = "kv"
	FeatureKvCas          Feature = "kvCas"
	FeatureKvCasFenced    Feature = "kvCasFenced"
	FeatureKvFencedLeases Feature = "kvFencedLeases"
	FeatureGraph          Feature = "graph"
	FeatureForks          Feature = "forks"
	FeatureWatch          Feature = "watch"
	FeatureAuthz          Feature = "authz"
	FeatureFilters        Feature = "filters"
	FeatureFilterCatalog  Feature = "filterCatalog"
)

const (
	// LocalConnectionString is the deployment an example reaches when the
	// environment names none.
	LocalConnectionString = "iggy:iggy@127.0.0.1:8090"
	// Partitions is how many partitions an example's topics are created with.
	// More than one, so an example exercises the ordering rule rather than
	// accidentally getting total order from a single partition.
	Partitions uint32 = 4
	// ProjectorTimeout bounds a wait on the managed projector.
	ProjectorTimeout = 60 * time.Second
	// ProjectionPoll is the pause between two looks at a view.
	ProjectionPoll = 150 * time.Millisecond

	defaultStreamPrefix = "laser"
	xorshiftMultiplier  = 0x2545f4914f6cdd1d
)

// runToken suffixes every managed index name of this invocation, so two runs
// never count each other's rows.
var runToken = fmt.Sprintf("%04x%04x", os.Getpid()&0xffff, time.Now().UnixMilli()&0xffff)

// RunExample connects to the example's own stream, emptied of the previous
// run, and runs body until it returns or the process is interrupted.
func RunExample(name string, body func(context.Context, *laser.Laser) error) {
	if err := runExample(name, body); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
}

// Run wires an example into a main: it connects, bounds the run, and reports a
// failure on the way out rather than panicking.
func Run(name string, body func(context.Context, *laser.Laser) error) {
	if err := run(name, body); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
}

// RunScratch creates a fresh stream for examples that need exclusive consumer groups.
func RunScratch(name string, body func(context.Context, *laser.Laser) error) {
	Run(name, func(ctx context.Context, client *laser.Laser) (err error) {
		scratch, err := client.ScratchStream(ctx, "laser-"+name+"-go")
		if err != nil {
			return err
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			err = errors.Join(err, scratch.Delete(cleanup))
		}()
		return body(ctx, scratch.Laser())
	})
}

// Connect opens the deployment an example runs against.
//
// It reads the environment rather than taking a flag, so the same binary runs
// against a local server, Laser Stack and LaserData Cloud without being
// rebuilt: LASER_CONNECTION_STRING, or LASER_SERVER with LASER_TOKEN or
// LASER_USERNAME and LASER_PASSWORD, or the local default.
func Connect(ctx context.Context, example string) (*laser.Laser, error) {
	options := laser.Options{DefaultStream: StreamFor(example)}
	if envValue("LASER_CONNECTION_STRING") != "" {
		return laser.ConnectEnvWith(ctx, options)
	}
	server := envValue("LASER_SERVER")
	if server == "" {
		return laser.ConnectWith(ctx, LocalConnectionString, options)
	}
	if token := envValue("LASER_TOKEN"); token != "" {
		return laser.ConnectWith(ctx, "iggy+tcp://"+token+"@"+server, options)
	}
	options.Address = server
	options.Username = envValue("LASER_USERNAME")
	options.Password = envValue("LASER_PASSWORD")
	if options.Username == "" || options.Password == "" {
		return nil, errors.New(
			"LaserData Cloud needs credentials: set LASER_TOKEN, or LASER_USERNAME + LASER_PASSWORD")
	}
	return laser.ConnectWith(ctx, "", options)
}

// ConnectExample opens the example's stream as a new run: the previous run's
// stream is deleted unless LASER_STREAM names a provisioned one.
func ConnectExample(ctx context.Context, example string) (*laser.Laser, error) {
	client, err := Connect(ctx, example)
	if err != nil {
		return nil, err
	}
	stream := client.Stream(StreamFor(example))
	if envValue("LASER_STREAM") == "" {
		if _, err := stream.Delete(ctx); err != nil {
			return nil, errors.Join(fmt.Errorf("reset stream: %w", err), client.Close())
		}
	}
	if err := stream.Ensure(ctx); err != nil {
		return nil, errors.Join(fmt.Errorf("ensure stream: %w", err), client.Close())
	}
	return client, nil
}

// StreamFor is the stream an example named `example` runs against:
// LASER_STREAM if set, otherwise "laser-<example>-go". The name is stable, so
// the result stays on the server after a run and a rerun starts clean.
func StreamFor(example string) string {
	if stream := envValue("LASER_STREAM"); stream != "" {
		return stream
	}
	return fmt.Sprintf("%s-%s-go", defaultStreamPrefix, example)
}

// RunToken is the per-invocation suffix shared by this run's group and index names.
func RunToken() string { return runToken }

// IndexFor names a managed index owned by this invocation, so a repeat or
// concurrent run materializes its own rows.
func IndexFor(base string) string { return base + "_" + runToken }

// Phase prints the heading every language's examples share.
func Phase(title string) {
	rule := strings.Repeat("─", utf8.RuneCountInString(title)+3)
	fmt.Printf("\n\x1b[1;36m▸ %s\x1b[0m\n\x1b[36m%s\x1b[0m\n", title, rule)
}

// ManagedGate reports whether the deployment serves feature, and says how to
// reach one that does when it does not.
func ManagedGate(capabilities laser.Capabilities, feature Feature, example string, label ...string) bool {
	if featureAvailable(capabilities, feature) {
		return true
	}
	name := string(feature)
	if len(label) > 0 {
		name = label[0]
	}
	fmt.Printf("\n  %s is unavailable on this deployment.\n  Use Laser Stack or LaserData Cloud:\n\n"+
		"    LASER_CONNECTION_STRING=user:password@host go run ./%s\n\n", name, example)
	return false
}

// Messages is LASER_MESSAGES, at least one, or fallback.
func Messages(fallback int) int { return max(1, EnvInt("LASER_MESSAGES", fallback)) }

// BatchSize is LASER_BATCH, at least one, or fallback.
func BatchSize(fallback int) int { return max(1, EnvInt("LASER_BATCH", fallback)) }

// EnvInt reads an integer setting, or fallback when it is unset or malformed.
func EnvInt(name string, fallback int) int {
	value, err := strconv.Atoi(envValue(name))
	if err != nil {
		return fallback
	}
	return value
}

// EnvBool reads a switch, or fallback when it is unset.
func EnvBool(name string, fallback bool) bool {
	switch strings.ToLower(envValue(name)) {
	case "":
		return fallback
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// Rng is the xorshift generator every language's examples seed alike, so they
// publish the same feed.
type Rng struct{ state uint64 }

// NewRng seeds a generator.
func NewRng(seed uint64) *Rng { return &Rng{state: seed | 1} }

// Next is the next 64-bit value.
func (rng *Rng) Next() uint64 {
	value := rng.state
	value ^= value << 13
	value ^= value >> 7
	value ^= value << 17
	rng.state = value
	return value * xorshiftMultiplier
}

// Below is a value in [0, bound).
func (rng *Rng) Below(bound int) int { return int(rng.Next() % uint64(bound)) }

// Pick chooses one of values.
func Pick[T any](rng *Rng, values []T) T { return values[rng.Below(len(values))] }

// PrintTable prints rows in aligned columns, the named columns right-aligned.
func PrintTable(rows [][]string, rightAligned ...int) {
	var widths []int
	for _, row := range rows {
		for column, cell := range row {
			if column == len(widths) {
				widths = append(widths, 0)
			}
			widths[column] = max(widths[column], utf8.RuneCountInString(cell))
		}
	}
	right := make(map[int]bool, len(rightAligned))
	for _, column := range rightAligned {
		right[column] = true
	}
	for _, row := range rows {
		cells := make([]string, len(row))
		for column, cell := range row {
			padding := strings.Repeat(" ", widths[column]-utf8.RuneCountInString(cell))
			if right[column] {
				cells[column] = padding + cell
			} else {
				cells[column] = cell + padding
			}
		}
		fmt.Println("  " + strings.TrimRight(strings.Join(cells, "  "), " "))
	}
}

// ViewProjection is the row projection an example declares for index over the
// named JSON fields.
func ViewProjection(index string, contentType wire.ContentType, inlinePayload bool, fields ...string) wire.Projection {
	extracted := make([]wire.IndexField, len(fields))
	for i, name := range fields {
		extracted[i] = wire.NewIndexField(name, "/"+name)
	}
	return wire.Projection{
		ID:                   wire.ProjectionID(index + ".v1"),
		Name:                 index,
		Version:              1,
		Kind:                 wire.ProjectionRow,
		History:              wire.AppendHistory(),
		ContentType:          contentType,
		Extraction:           wire.IndexSchema{Fields: extracted, InlinePayload: inlinePayload},
		InlinePayloadDefault: inlinePayload,
	}
}

// RegisterView registers projection and binds it to topic on the example's
// stream, with change notices on.
func RegisterView(
	ctx context.Context, client *laser.Laser, topic string, projection wire.Projection,
	invalidRecords wire.InvalidRecordPolicy,
) error {
	if err := client.Projections().Register(ctx, projection); err != nil {
		return fmt.Errorf("register projection %s: %w", projection.ID, err)
	}
	binding := wire.ProjectionBinding{
		Source:             wire.NewSourceSelector(client.DefaultStream(), topic),
		AllowedProjections: []wire.ProjectionID{projection.ID},
		DefaultProjection:  &projection.ID,
		Index:              projection.Name,
		Notify:             true,
		InvalidRecords:     invalidRecords,
		Oversize:           wire.OversizeBlock,
	}
	if err := client.Bindings().Apply(ctx, binding); err != nil {
		return fmt.Errorf("bind %s to %s: %w", projection.Name, topic, err)
	}
	return nil
}

// EnsureView declares index over topic and waits until it answers queries, so
// a publish that follows flows into a live projector.
func EnsureView(ctx context.Context, client *laser.Laser, topic, index string, fields ...string) error {
	projection := ViewProjection(index, wire.ContentJSON, false, fields...)
	if err := RegisterView(ctx, client, topic, projection, wire.InvalidRecordBlock); err != nil {
		return err
	}
	deadline := time.Now().Add(ProjectorTimeout)
	for {
		_, err := client.Query(index).Fetch(ctx)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("view %s never answered: %w", index, err)
		}
		if err := pause(ctx, ProjectionPoll); err != nil {
			return err
		}
	}
}

// WaitForSchema waits until the registry serves a registered writer schema.
func WaitForSchema(ctx context.Context, client *laser.Laser, id uint32) error {
	deadline := time.Now().Add(ProjectorTimeout)
	for time.Now().Before(deadline) {
		info, err := client.Schemas().Get(ctx, id)
		if err != nil {
			return fmt.Errorf("read schema %d: %w", id, err)
		}
		if info != nil {
			return nil
		}
		if err := pause(ctx, ProjectionPoll); err != nil {
			return err
		}
	}
	return fmt.Errorf("schema %d never appeared in the registry", id)
}

// WaitForProjection waits until index holds expected rows and returns the
// total, following the change feed where the deployment serves one.
func WaitForProjection(ctx context.Context, client *laser.Laser, index string, expected uint64) (uint64, error) {
	deadline := time.Now().Add(ProjectorTimeout)
	var feed *laser.WatchReader
	if client.Capabilities().Watch {
		reader, err := client.Watch().Index(index).Records(ctx)
		if err != nil {
			return 0, fmt.Errorf("watch %s: %w", index, err)
		}
		feed = reader
	}
	last, seen := uint64(0), false
	for {
		advanced := !seen || feed == nil
		if !advanced {
			changes, err := feed.Poll(ctx)
			if err != nil {
				return 0, fmt.Errorf("watch %s: %w", index, err)
			}
			advanced = len(changes) > 0
		}
		if advanced {
			total := uint64(0)
			if result, err := client.Query(index).WithTotal().Fetch(ctx); err == nil && result.Page.Total != nil {
				total = *result.Page.Total
			}
			if !seen || total != last {
				fmt.Printf("  projector materialized %d/%d rows\n", total, expected)
				last, seen = total, true
			}
			if total >= expected {
				return total, nil
			}
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("projector indexed only %d/%d rows in `%s` before the deadline", last, expected, index)
		}
		if err := pause(ctx, ProjectionPoll); err != nil {
			return 0, err
		}
	}
}

func runExample(name string, body func(context.Context, *laser.Laser) error) (err error) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client, err := ConnectExample(ctx, name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	return body(ctx, client)
}

func run(name string, body func(context.Context, *laser.Laser) error) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := Connect(ctx, name)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, client.Close()) }()
	fmt.Printf("%s: connected, managed backend: %v\n", name, client.Capabilities().Managed)
	return body(ctx, client)
}

func featureAvailable(capabilities laser.Capabilities, feature Feature) bool {
	switch feature {
	case FeatureManaged:
		return capabilities.Managed
	case FeatureQuery:
		return capabilities.Query.Available
	case FeatureKv:
		return capabilities.Kv.Available
	case FeatureKvCas:
		return capabilities.Kv.Available && capabilities.Kv.CAS
	case FeatureKvCasFenced:
		return capabilities.Kv.Available && capabilities.Kv.CASFenced
	case FeatureKvFencedLeases:
		return capabilities.Kv.Available && capabilities.Kv.FencedLeases
	case FeatureGraph:
		return capabilities.Graph
	case FeatureForks:
		return capabilities.Forks
	case FeatureWatch:
		return capabilities.Watch
	case FeatureAuthz:
		return capabilities.Authz
	case FeatureFilters:
		return capabilities.Filters.Native
	case FeatureFilterCatalog:
		return capabilities.Filters.Catalog
	default:
		return false
	}
}

func envValue(name string) string { return strings.TrimSpace(os.Getenv(name)) }

func pause(ctx context.Context, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
