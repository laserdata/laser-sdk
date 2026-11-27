package laser

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
)

// StateStoreError reports a local store that could not be read or written.
type StateStoreError struct{ Reason string }

func (e *StateStoreError) Error() string { return "state store: " + e.Reason }

// StateStore is local durable state beside the log: dedup keys, cursors, and
// whatever else a process needs to survive its own restart.
//
// It is deliberately a seam rather than an implementation. What durable means
// differs by deployment, and a client that chose for everyone would be wrong
// for most of them.
type StateStore interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
}

// InMemoryStore keeps state for the life of the process.
//
// It is what a test wants and what a process that can rebuild its state from
// the log wants. It is not durable, and nothing here pretends otherwise.
type InMemoryStore struct {
	guard  sync.RWMutex
	values map[string][]byte
}

// NewInMemoryStore opens an empty in-memory store.
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{values: map[string][]byte{}}
}

// Get reads a key, reporting whether one was there.
func (store *InMemoryStore) Get(
	_ context.Context, key string,
) ([]byte, bool, error) {
	if err := validStateKey(key); err != nil {
		return nil, false, err
	}
	store.guard.RLock()
	defer store.guard.RUnlock()
	value, present := store.values[key]
	if !present {
		return nil, false, nil
	}
	// A copy, so a caller mutating what it read cannot change what is stored.
	return append([]byte(nil), value...), true, nil
}

// Set writes a key.
func (store *InMemoryStore) Set(_ context.Context, key string, value []byte) error {
	if err := validStateKey(key); err != nil {
		return err
	}
	store.guard.Lock()
	defer store.guard.Unlock()
	if store.values == nil {
		store.values = map[string][]byte{}
	}
	store.values[key] = append([]byte(nil), value...)
	return nil
}

// Delete removes a key. Removing one that is not there is what the caller
// asked for, so it is not an error.
func (store *InMemoryStore) Delete(_ context.Context, key string) error {
	if err := validStateKey(key); err != nil {
		return err
	}
	store.guard.Lock()
	defer store.guard.Unlock()
	delete(store.values, key)
	return nil
}

// FileStore keeps state under one directory, for the on-box disk a
// deployment mounts.
//
// Keys are hex-encoded into file names, so any key is safe and no key can
// traverse out of the directory however it was constructed.
type FileStore struct{ root string }

// NewFileStore opens a store rooted at a directory.
func NewFileStore(root string) *FileStore { return &FileStore{root: root} }

// Root is where this store keeps its files.
func (store *FileStore) Root() string { return store.root }

// pathFor is the file one key lives in.
func (store *FileStore) pathFor(key string) string {
	return filepath.Join(store.root, hex.EncodeToString([]byte(key)))
}

// Get reads a key, reporting whether one was there.
func (store *FileStore) Get(
	_ context.Context, key string,
) ([]byte, bool, error) {
	if err := validStateKey(key); err != nil {
		return nil, false, err
	}
	value, err := os.ReadFile(store.pathFor(key))
	switch {
	case err == nil:
		return value, true, nil
	case os.IsNotExist(err):
		return nil, false, nil
	default:
		return nil, false, &StateStoreError{Reason: err.Error()}
	}
}

// Set writes a key durably.
//
// The bytes are staged in a file of their own and renamed into place, because
// a crash partway through a plain write leaves a partial or empty file that
// the next read returns as though it were valid state. The staged bytes are
// flushed before the rename, since a rename can otherwise reach disk before
// the data it renames, and the directory is flushed after, so the rename
// itself survives.
//
// The staging name carries an identity of its own, so two writers on one key
// cannot clobber each other's staging file: whichever renames last wins, and
// each rename replaces the previous one whole.
func (store *FileStore) Set(_ context.Context, key string, value []byte) error {
	if err := validStateKey(key); err != nil {
		return err
	}
	if err := os.MkdirAll(store.root, ownerOnlyDirectory); err != nil {
		return &StateStoreError{Reason: err.Error()}
	}
	// Dedup keys, cursors and conversation state are this store's content, so
	// the directory is restricted to its owner rather than left at whatever
	// the process umask happens to be.
	if err := os.Chmod(store.root, ownerOnlyDirectory); err != nil {
		return &StateStoreError{Reason: err.Error()}
	}
	final := store.pathFor(key)
	identity := NewOperationID()
	staged := final + "." + hex.EncodeToString(identity[:]) + ".tmp"
	file, err := os.OpenFile(staged,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL, ownerOnlyFile)
	if err != nil {
		return &StateStoreError{Reason: err.Error()}
	}
	if err := writeAndSync(file, value); err != nil {
		os.Remove(staged)
		return err
	}
	if err := os.Rename(staged, final); err != nil {
		os.Remove(staged)
		return &StateStoreError{Reason: err.Error()}
	}
	if directory, err := os.Open(store.root); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return nil
}

// writeAndSync writes the bytes and flushes them to disk before the file is
// closed.
func writeAndSync(file *os.File, value []byte) error {
	defer file.Close()
	if _, err := file.Write(value); err != nil {
		return &StateStoreError{Reason: err.Error()}
	}
	if err := file.Sync(); err != nil {
		return &StateStoreError{Reason: err.Error()}
	}
	return nil
}

// Delete removes a key. Removing one that is not there is what the caller
// asked for.
func (store *FileStore) Delete(_ context.Context, key string) error {
	if err := validStateKey(key); err != nil {
		return err
	}
	switch err := os.Remove(store.pathFor(key)); {
	case err == nil, os.IsNotExist(err):
		return nil
	default:
		return &StateStoreError{Reason: err.Error()}
	}
}

// validStateKey refuses a key that names nothing.
//
// An empty key encodes to an empty file name, which is the directory itself:
// storing under it would try to replace the store with one of its own values.
// Both stores refuse it, so swapping one for the other cannot change what a
// caller gets.
func validStateKey(key string) error {
	if key == "" {
		return &StateStoreError{Reason: "a key must not be empty"}
	}
	return nil
}

// The permissions this store's files and directory take: the owner's alone,
// because what they hold is this process's own state rather than anything
// another user on the box has business reading.
const (
	ownerOnlyDirectory os.FileMode = 0o700
	ownerOnlyFile      os.FileMode = 0o600
)
