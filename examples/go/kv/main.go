package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

const (
	example   = "kv"
	namespace = "config"
	key       = "service:auth"
	ttl       = 24 * time.Hour
	forkName  = "experiment-1"
	leaseKey  = "lease:service:auth"
	holder    = "worker-a"
	leaseTTL  = 30 * time.Second
)

type config struct {
	LogLevel string `json:"log_level"`
}

func main() { examples.RunExample(example, run) }

func run(ctx context.Context, client *laser.Laser) error {
	capabilities := client.Capabilities()
	if !examples.ManagedGate(capabilities, examples.FeatureKv, example) {
		return nil
	}
	kv := client.Kv(namespace)

	examples.Phase("set and get keyed state")
	if err := kv.Set([]byte(key)).JSON(config{LogLevel: "info"}).TTL(ttl).Send(ctx); err != nil {
		return fmt.Errorf("set %s: %w", key, err)
	}
	level, err := currentLevel(ctx, kv)
	if err != nil {
		return err
	}
	fmt.Printf("  %s logs at %s\n", key, level)

	if examples.ManagedGate(capabilities, examples.FeatureKvCas, example) {
		if err := compareAndSwap(ctx, kv); err != nil {
			return err
		}
	}
	if examples.ManagedGate(capabilities, examples.FeatureKvFencedLeases, example) {
		if err := leaseAndFencedWrite(ctx, kv); err != nil {
			return err
		}
	}
	if examples.ManagedGate(capabilities, examples.FeatureForks, example) {
		if err := forkAndPromote(ctx, client); err != nil {
			return err
		}
	}
	return nil
}

func compareAndSwap(ctx context.Context, kv *laser.Kv) error {
	examples.Phase("compare-and-swap: the write lands only if nobody moved first")
	entry, err := kv.GetEntry(ctx, []byte(key))
	if err != nil {
		return fmt.Errorf("read %s: %w", key, err)
	}
	if entry == nil {
		return fmt.Errorf("%s vanished", key)
	}
	_, err = kv.Set([]byte(key)).JSON(config{LogLevel: "debug"}).ExpectVersion(entry.Version).Commit(ctx)
	if err != nil {
		return fmt.Errorf("compare-and-swap %s: %w", key, err)
	}
	raised, err := currentLevel(ctx, kv)
	if err != nil {
		return err
	}
	fmt.Printf("  version %d accepted the change, %s now logs at %s\n", entry.Version, key, raised)
	return nil
}

func leaseAndFencedWrite(ctx context.Context, kv *laser.Kv) error {
	examples.Phase("lease and fenced write: at most one effective writer")
	lease, err := kv.Lease(ctx, []byte(leaseKey), holder, leaseTTL)
	if err != nil {
		return fmt.Errorf("acquire %s: %w", leaseKey, err)
	}
	fmt.Printf("  %s holds %s at fence %d\n", holder, leaseKey, lease.Token)
	// Barriered read: the answering fold has applied at least the grant, so a
	// holder that just took over never plans against its predecessor's state.
	held, err := kv.GetEntryAtLeast(ctx, []byte(key), lease.Position)
	if err != nil {
		return fmt.Errorf("barriered read of %s: %w", key, err)
	}
	if held == nil {
		return fmt.Errorf("%s vanished", key)
	}
	fenced, err := kv.CasFenced([]byte(key), namespace, []byte(leaseKey), lease.Token).
		JSON(config{LogLevel: "trace"}).TTL(ttl).ExpectVersion(held.Version).Commit(ctx)
	if err != nil {
		return fmt.Errorf("fenced write of %s: %w", key, err)
	}
	fmt.Printf("  barriered read saw %s, the fenced write landed as version %d\n", levelOf(held.Value, true), fenced)
	renewed, err := kv.RenewLease(ctx, []byte(leaseKey), holder, lease.Token, leaseTTL)
	if err != nil {
		return fmt.Errorf("renew %s: %w", leaseKey, err)
	}
	if _, err := kv.ReleaseLease(ctx, []byte(leaseKey), holder, renewed.Token); err != nil {
		return fmt.Errorf("release %s: %w", leaseKey, err)
	}
	fmt.Printf("  lease renewed at the same fence %d, then released\n", renewed.Token)
	// The gate holds without waiting for a successor: a released fence is
	// already dead, so a zombie holder cannot commit through it.
	_, err = kv.CasFenced([]byte(key), namespace, []byte(leaseKey), lease.Token).
		JSON(config{LogLevel: "zombie"}).ExpectVersion(fenced).Commit(ctx)
	if err == nil {
		return errors.New("a released fence was accepted")
	}
	if !laser.IsLeaseLost(err) {
		return fmt.Errorf("write through a released fence: %w", err)
	}
	fmt.Println("  after release the same fence is refused: lease-lost")
	return nil
}

func forkAndPromote(ctx context.Context, client *laser.Laser) error {
	examples.Phase("fork: a branch of the same state, promoted or thrown away")
	table := examples.IndexFor(namespace)
	if err := client.Topic(namespace).Ensure(ctx, examples.Partitions, nil); err != nil {
		return fmt.Errorf("ensure %s: %w", namespace, err)
	}
	if err := examples.EnsureView(ctx, client, namespace, table, "log_level"); err != nil {
		return err
	}
	fork := client.Fork(forkName)
	if _, err := fork.Squash(ctx); err != nil {
		return fmt.Errorf("squash an earlier fork: %w", err)
	}
	if _, err := fork.Create().Severed().Tables(table).Send(ctx); err != nil {
		return fmt.Errorf("create fork %s: %w", forkName, err)
	}
	if err := fork.PutRow(table, 0, 0).Field("log_level", "trace-preview").Send(ctx); err != nil {
		return fmt.Errorf("stage a row in %s: %w", forkName, err)
	}
	applied, err := fork.Promote(ctx)
	if err != nil {
		return fmt.Errorf("promote fork %s: %w", forkName, err)
	}
	fmt.Printf("  fork `%s` promoted, %d row(s) applied\n", forkName, applied)
	return nil
}

func currentLevel(ctx context.Context, kv *laser.Kv) (string, error) {
	value, found, err := kv.Get(ctx, []byte(key))
	if err != nil {
		return "", fmt.Errorf("read %s: %w", key, err)
	}
	return levelOf(value, found), nil
}

func levelOf(value []byte, found bool) string {
	var stored config
	if !found || json.Unmarshal(value, &stored) != nil || stored.LogLevel == "" {
		return "no level"
	}
	return stored.LogLevel
}
