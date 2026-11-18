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
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// Partitions is how many partitions an example's topics are created with.
// More than one, so an example exercises the ordering rule rather than
// accidentally getting total order from a single partition.
const Partitions uint32 = 4

// Connect opens the deployment an example runs against.
//
// It reads the environment rather than taking a flag, so the same binary runs
// against a local container, a staging stack and LaserData Cloud without being
// rebuilt.
func Connect(ctx context.Context) (*laser.Laser, error) {
	if _, set := os.LookupEnv("LASER_CONNECTION_STRING"); !set {
		return nil, fmt.Errorf(
			"set LASER_CONNECTION_STRING to the deployment to run against, " +
				"for example iggy:iggy@127.0.0.1:8090")
	}
	return laser.ConnectEnv(ctx)
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

	client, err := Connect(ctx)
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
