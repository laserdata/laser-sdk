// The managed key-value store: read, write, expire, and a compare-and-swap
// that two writers race on.
//
// The race is the point. Optimistic concurrency means both writers read the
// same version, both try to write it, and exactly one wins. The loser is told
// so rather than silently overwriting, which is what a store without a
// conditional write would do.
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	examples "github.com/laserdata/laser-sdk/examples/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

const (
	namespace = "config"
	key       = "service:auth"
)

func main() { examples.Run("kv", run) }

func run(ctx context.Context, client *laser.Laser) error {
	if err := examples.RequireManaged(client, "the key-value store"); err != nil {
		return err
	}
	store := client.Kv(namespace)

	examples.Phase("write a value with a lifetime")
	if err := store.Set([]byte(key)).
		JSON(map[string]string{"log_level": "info"}).
		TTL(24 * time.Hour).
		Send(ctx); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	fmt.Printf("wrote %s/%s, expiring in 24h\n", namespace, key)

	examples.Phase("read it back")
	entry, err := store.GetEntry(ctx, []byte(key))
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}
	if entry == nil {
		return errors.New("the value written a moment ago is not there")
	}
	fmt.Printf("read version %d: %s\n", entry.Version, entry.Value)

	examples.Phase("two writers race on one key")
	// Both read the same version, which is what makes this a race rather than
	// a sequence.
	version := entry.Version

	winner, err := store.Set([]byte(key)).
		JSON(map[string]string{"log_level": "debug"}).
		ExpectVersion(version).
		Commit(ctx)
	if err != nil {
		return fmt.Errorf("the first writer should have won: %w", err)
	}
	fmt.Printf("writer one committed, version is now %d\n", winner)

	_, err = store.Set([]byte(key)).
		JSON(map[string]string{"log_level": "trace"}).
		ExpectVersion(version).
		Commit(ctx)

	var conflict *laser.KvError
	switch {
	case err == nil:
		return errors.New("both writers won, which the store must not allow")
	case errors.As(err, &conflict) && conflict.Conflict():
		// This is the whole point: the loser is told it lost, so it can
		// re-read and decide, rather than having quietly overwritten the
		// winner.
		fmt.Println("writer two was refused: its version was already superseded")
	default:
		return fmt.Errorf("writer two failed for the wrong reason: %w", err)
	}

	examples.Phase("a conditional read skips the body when nothing changed")
	revalidation, err := store.GetEntryIfChanged(ctx, []byte(key), winner)
	if err != nil {
		return fmt.Errorf("conditional read: %w", err)
	}
	if revalidation.NotModified {
		fmt.Println("not modified, so the value did not come back over the wire")
	} else {
		fmt.Printf("changed again, now version %d\n", revalidation.Entry.Version)
	}

	examples.Phase("list what this namespace holds")
	page, err := store.Scan().Prefix([]byte("service:")).Fetch(ctx)
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}
	for _, item := range page.Items {
		fmt.Printf("  %s (version %d)\n", item.Key, item.Version)
	}

	examples.Phase("clean up")
	removed, err := store.Delete(ctx, []byte(key))
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}
	fmt.Printf("removed: %v\n", removed)
	return nil
}
