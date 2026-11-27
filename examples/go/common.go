// Package examples holds the pieces the Go examples share.
//
// Each example is a standalone program under its own directory, run against a
// deployment named by LASER_CONNECTION_STRING. They are written to be read as
// much as run, so each one does one thing and says why.
package examples

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// Partitions is how many partitions an example's topics are created with.
// More than one, so an example exercises the ordering rule rather than
// accidentally getting total order from a single partition.
const Partitions uint32 = 4

// defaultStreamPrefix names the stream an example uses when LASER_STREAM is
// unset, matching the Rust and TypeScript examples' own per-example default
// (`laser-<example>-rust`, `laser-<example>-ts`) so the same instructions
// ("set LASER_CONNECTION_STRING") work across every SDK's examples.
const defaultStreamPrefix = "laser"

// StreamFor is the stream an example named `example` runs against:
// LASER_STREAM if set (a provisioned stream, so the SDK auto-creates nothing
// and repeat runs share its state), else "laser-<example>-go". Exported so a
// caller resolving a connection outside Connect/Run sees the same default.
func StreamFor(example string) string {
	if stream, set := os.LookupEnv("LASER_STREAM"); set && strings.TrimSpace(stream) != "" {
		return stream
	}
	return fmt.Sprintf("%s-%s-go", defaultStreamPrefix, example)
}

// Connect opens the deployment an example runs against.
//
// It reads the environment rather than taking a flag, so the same binary runs
// against a local container, a staging stack and LaserData Cloud without being
// rebuilt. The stream defaults per StreamFor, so running with only
// LASER_CONNECTION_STRING set (no LASER_STREAM) still has a stream to
// bootstrap against, rather than failing with "no stream selected."
func Connect(ctx context.Context, example string) (*laser.Laser, error) {
	if _, set := os.LookupEnv("LASER_CONNECTION_STRING"); !set {
		return nil, fmt.Errorf(
			"set LASER_CONNECTION_STRING to the deployment to run against, " +
				"for example iggy:iggy@127.0.0.1:8090")
	}
	return laser.ConnectEnvWith(ctx, laser.Options{DefaultStream: StreamFor(example)})
}

// Phase prints a step so the output reads as a narrative rather than a dump.
func Phase(name string) {
	fmt.Printf("\n== %s\n", name)
}

// Run wires an example into a main: it connects, bounds the run, and reports a
// failure on the way out rather than panicking.
func Run(name string, body func(context.Context, *laser.Laser) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := Connect(ctx, name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Printf("%s: connected, managed backend: %v\n",
		name, client.Capabilities().Managed)
	if err := body(ctx, client); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
		os.Exit(1)
	}
	fmt.Printf("\n%s: done\n", name)
}

// RequireManaged stops an example that needs a managed backend, naming what is
// missing rather than failing later on an Unsupported error.
func RequireManaged(client *laser.Laser, surface string) error {
	if !client.Capabilities().Managed {
		return fmt.Errorf(
			"%s needs a managed backend, which this deployment does not have: "+
				"run against Laser Stack or LaserData Cloud", surface)
	}
	return nil
}
