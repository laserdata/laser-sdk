package laser_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// Both stores answer the same contract, so a caller swaps one for the other
// without changing anything else.
func TestGivenEitherStoreWhenUsedThenShouldKeepWhatItWasGiven(t *testing.T) {
	for name, open := range map[string]func(*testing.T) laser.StateStore{
		"in memory": func(*testing.T) laser.StateStore {
			return laser.NewInMemoryStore()
		},
		"on disk": func(t *testing.T) laser.StateStore {
			return laser.NewFileStore(t.TempDir())
		},
	} {
		t.Run(name, func(t *testing.T) {
			store := open(t)
			ctx := context.Background()

			// A key nobody wrote is absent rather than empty, so a caller can
			// tell a stored empty value from no value at all.
			value, present, err := store.Get(ctx, "cursor")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if present || value != nil {
				t.Errorf("want it absent, got %q", value)
			}

			if err := store.Set(ctx, "cursor", []byte("at 42")); err != nil {
				t.Fatalf("set: %v", err)
			}
			value, present, err = store.Get(ctx, "cursor")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if !present || string(value) != "at 42" {
				t.Errorf("want what was written, got %q", value)
			}

			// A stored empty value is a value.
			if err := store.Set(ctx, "empty", nil); err != nil {
				t.Fatalf("set: %v", err)
			}
			value, present, err = store.Get(ctx, "empty")
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if !present || len(value) != 0 {
				t.Errorf("want a stored empty value, got %q and %v", value, present)
			}

			// A write replaces what was there.
			if err := store.Set(ctx, "cursor", []byte("at 99")); err != nil {
				t.Fatalf("set: %v", err)
			}
			value, _, _ = store.Get(ctx, "cursor")
			if string(value) != "at 99" {
				t.Errorf("want the later write, got %q", value)
			}

			if err := store.Delete(ctx, "cursor"); err != nil {
				t.Fatalf("delete: %v", err)
			}
			if _, present, _ := store.Get(ctx, "cursor"); present {
				t.Error("want it gone")
			}
			// Deleting what is not there is what the caller asked for.
			if err := store.Delete(ctx, "cursor"); err != nil {
				t.Errorf("deleting twice must succeed: %v", err)
			}
		})
	}
}

// A caller mutating what it read must not change what is stored, or two
// readers would see each other's edits.
func TestGivenAReadValueWhenMutatedThenShouldNotChangeTheStore(t *testing.T) {
	store := laser.NewInMemoryStore()
	ctx := context.Background()
	if err := store.Set(ctx, "k", []byte("original")); err != nil {
		t.Fatalf("set: %v", err)
	}

	read, _, err := store.Get(ctx, "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	read[0] = 'X'

	again, _, err := store.Get(ctx, "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(again) != "original" {
		t.Errorf("want the store unchanged, got %q", again)
	}

	// The same holds the other way: editing what was written must not change
	// what is stored.
	written := []byte("written")
	if err := store.Set(ctx, "w", written); err != nil {
		t.Fatalf("set: %v", err)
	}
	written[0] = 'X'
	stored, _, _ := store.Get(ctx, "w")
	if string(stored) != "written" {
		t.Errorf("want the stored copy unchanged, got %q", stored)
	}
}

// Keys are encoded into file names, so any key is safe and none can traverse
// out of the directory however it was constructed.
func TestGivenAHostileKeyWhenStoredThenShouldStayInsideTheDirectory(t *testing.T) {
	root := t.TempDir()
	store := laser.NewFileStore(root)
	ctx := context.Background()

	for _, key := range []string{
		"../escape", "../../etc/passwd", "with/slashes", "with spaces",
		"\x00null", ".", "..",
	} {
		if err := store.Set(ctx, key, []byte("value")); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
		value, present, err := store.Get(ctx, key)
		if err != nil || !present || string(value) != "value" {
			t.Errorf("%q: want it stored and read back, got %q %v %v",
				key, value, present, err)
		}
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read the directory: %v", err)
	}
	for _, entry := range entries {
		if strings.ContainsAny(entry.Name(), `/\. `) {
			t.Errorf("want an encoded name, got %q", entry.Name())
		}
	}
	// Nothing landed outside.
	if _, err := os.Stat(filepath.Join(root, "..", "escape")); err == nil {
		t.Error("a key must not traverse out of the directory")
	}
}

// A key that names nothing encodes to a file name that names nothing, which
// is the directory itself. Both stores refuse it, so swapping one for the
// other cannot change what a caller gets.
func TestGivenAnEmptyKeyWhenStoredThenShouldRefuseIt(t *testing.T) {
	for name, store := range map[string]laser.StateStore{
		"in memory": laser.NewInMemoryStore(),
		"on disk":   laser.NewFileStore(t.TempDir()),
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			var failure *laser.StateStoreError

			if err := store.Set(ctx, "", []byte("v")); !errors.As(err, &failure) {
				t.Errorf("set: want a StateStoreError, got %T: %v", err, err)
			}
			if _, _, err := store.Get(ctx, ""); !errors.As(err, &failure) {
				t.Errorf("get: want a StateStoreError, got %T: %v", err, err)
			}
			if err := store.Delete(ctx, ""); !errors.As(err, &failure) {
				t.Errorf("delete: want a StateStoreError, got %T: %v", err, err)
			}
		})
	}
}

// Dedup keys, cursors and conversation state are this store's content, so the
// directory is the owner's alone rather than whatever the umask gives.
func TestGivenAFileStoreWhenWrittenThenShouldRestrictItToItsOwner(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	store := laser.NewFileStore(root)

	if err := store.Set(context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}

	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Errorf("want the directory owner-only, got %v", info.Mode().Perm())
	}

	// A directory that already exists with wider permissions is narrowed,
	// because state written into one anybody can read is state anybody can
	// read whoever created the directory.
	wide := filepath.Join(t.TempDir(), "wide")
	if err := os.MkdirAll(wide, 0o777); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.Chmod(wide, 0o777); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := laser.NewFileStore(wide).Set(
		context.Background(), "k", []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	widened, err := os.Stat(wide)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if widened.Mode().Perm() != 0o700 {
		t.Errorf("want an existing directory narrowed, got %v",
			widened.Mode().Perm())
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read the directory: %v", err)
	}
	for _, entry := range entries {
		file, err := entry.Info()
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if file.Mode().Perm() != 0o600 {
			t.Errorf("want the file owner-only, got %v", file.Mode().Perm())
		}
	}
}

// A write leaves no staging file behind, so the directory holds state rather
// than the wreckage of writing it.
func TestGivenRepeatedWritesWhenStoredThenShouldLeaveNoStagingFiles(t *testing.T) {
	root := t.TempDir()
	store := laser.NewFileStore(root)
	ctx := context.Background()

	for range 5 {
		if err := store.Set(ctx, "k", []byte("v")); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read the directory: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("want one file for one key, got %d", len(entries))
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".tmp") {
			t.Errorf("want no staging file left, got %q", entry.Name())
		}
	}
}

// Two writers on one key must not clobber each other's staging file: each
// stages under its own name and whichever renames last wins whole.
func TestGivenConcurrentWritersWhenStoringOneKeyThenShouldLeaveOneWholeValue(t *testing.T) {
	root := t.TempDir()
	store := laser.NewFileStore(root)
	ctx := context.Background()
	const writers = 16

	var writing sync.WaitGroup
	for writer := range writers {
		writing.Add(1)
		go func(writer int) {
			defer writing.Done()
			value := []byte(strings.Repeat(string(rune('a'+writer)), 512))
			if err := store.Set(ctx, "k", value); err != nil {
				t.Errorf("set: %v", err)
			}
		}(writer)
	}
	writing.Wait()

	value, present, err := store.Get(ctx, "k")
	if err != nil || !present {
		t.Fatalf("get: %v %v", err, present)
	}
	// One writer's value, whole: never a mixture and never a partial write.
	if len(value) != 512 {
		t.Fatalf("want one whole value, got %d bytes", len(value))
	}
	for _, character := range value {
		if character != value[0] {
			t.Fatal("want one writer's value rather than a mixture")
		}
	}
	entries, _ := os.ReadDir(root)
	if len(entries) != 1 {
		t.Errorf("want one file left, got %d", len(entries))
	}
}

// A store rooted somewhere it cannot write says so rather than silently
// losing the state it was given.
func TestGivenAnUnwritableRootWhenStoringThenShouldReportIt(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(blocked, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	store := laser.NewFileStore(filepath.Join(blocked, "state"))

	err := store.Set(context.Background(), "k", []byte("v"))

	var failure *laser.StateStoreError
	if !errors.As(err, &failure) {
		t.Fatalf("want a StateStoreError, got %T: %v", err, err)
	}
}

// The root is readable, so a caller can say where its state went.
func TestGivenAFileStoreWhenAskedThenShouldNameItsRoot(t *testing.T) {
	root := t.TempDir()

	if laser.NewFileStore(root).Root() != root {
		t.Error("want the root back")
	}
}
