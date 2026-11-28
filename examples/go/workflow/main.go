// go get github.com/laserdata/laser-sdk/foreign/go/laser@latest
// Needs only LASER_CONNECTION_STRING, which ConnectEnv reads. No server to start when it is set.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	client, err := laser.ConnectEnv(ctx)
	if err != nil {
		return err
	}
	defer client.Close()
	if client.DefaultStream() == "" {
		client = client.WithDefaultStream("pipeline")
	}
	source, err := wire.NewAgentID("pipeline")
	if err != nil {
		return err
	}
	outcome, err := laser.RunWith(ctx, client.Workflow(source, wire.AgentSessions), pipeline)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"status": outcome.Status, "steps": outcome.CompletedSteps, "report": json.RawMessage(outcome.Value),
	})
}

func pipeline(ctx context.Context, run *laser.WorkflowRun) (string, error) {
	frames, err := laser.RunStep(ctx, run, "ingest", func(context.Context) (string, error) {
		return "frames from sat-7", nil
	})
	if err != nil {
		return "", err
	}
	analysis, err := laser.RunStep(ctx, run, "analyze", func(context.Context) (string, error) {
		time.Sleep(time.Second)
		return "analyzed " + frames, nil
	})
	if err != nil {
		return "", err
	}
	return laser.RunStep(ctx, run, "report", func(context.Context) (string, error) {
		return "report on " + analysis, nil
	})
}
