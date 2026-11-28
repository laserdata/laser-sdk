package bdd_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func waitForSessionIndex(ctx context.Context, condition string, check func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := check()
		if err != nil && !sessionNotYetFolded(err) {
			return err
		}
		if err == nil && ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s: %w", condition, ctx.Err())
		case <-ticker.C:
		}
	}
}

func managedSessionIndexStep(ctx context.Context, state *world, flow *managedSessionFlow, text string, args []string) error {
	switch {
	case strings.HasPrefix(text, "agent ") && strings.Contains(text, " starts the session ") && !strings.Contains(text, " on the second stream") && len(args) == 2:
		owner, err := bridgeAgent(args[0])
		if err != nil {
			return err
		}
		session := flow.sessions.Create(args[1]).As(owner)
		if seconds, ok := after(text, "idle timeout of "); ok {
			session = session.WithIdleTimeout(time.Duration(seconds) * time.Second)
		}
		if tokens, ok := after(text, "budget of "); ok {
			session = session.WithBudget(wire.Budget{Tokens: &tokens})
		}
		_, lease, err := session.Begin(ctx)
		if err != nil {
			return err
		}
		if flow.config.Heartbeat > 0 {
			flow.lease = lease
			state.t.Cleanup(lease.Release)
		} else {
			lease.Release()
		}
		flow.session, flow.label = session, args[1]
		flow.byLabel[args[1]] = session
		return nil
	case strings.HasPrefix(text, "the indexed session ") && strings.HasSuffix(text, " is active"):
		session := flow.byLabel[args[0]]
		if session == nil {
			return fmt.Errorf("no session %q was started", args[0])
		}
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			row, err := flow.sessions.Get(ctx, session.Conversation())
			return row != nil && row.Status == wire.SessionActive, err
		})
	case strings.HasPrefix(text, "the session state sets "):
		return setStateOn(ctx, flow.session, text, args)
	case strings.HasPrefix(text, "the session state is replaced by "):
		var doc wire.DynValue
		if err := wire.UnmarshalJSON([]byte(strings.TrimPrefix(text, "the session state is replaced by ")), &doc); err != nil {
			return err
		}
		_, err := flow.session.State().Replace(ctx, doc)
		return err
	case text == "the session writes a state snapshot":
		_, err := flow.session.State().Snapshot(ctx)
		return err
	case strings.HasPrefix(text, "the indexed state of the session is "):
		var want wire.DynValue
		if err := wire.UnmarshalJSON([]byte(strings.TrimPrefix(text, "the indexed state of the session is ")), &want); err != nil {
			return err
		}
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			view, err := flow.sessions.State(ctx, flow.session.Conversation(), 0)
			return view != nil && view.Complete && view.Document.Equal(want), err
		})
	case strings.HasPrefix(text, "the indexed state history chains its digests over "):
		deltas, ok := after(text, "over ")
		if !ok {
			return fmt.Errorf("missing delta count")
		}
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			view, err := flow.sessions.State(ctx, flow.session.Conversation(), 0)
			if err != nil || view == nil || len(view.History) != int(deltas)+1 {
				return false, err
			}
			for i, change := range view.History {
				if change.Outcome != wire.StateApplied || change.OldDigest == nil || change.NewDigest == nil {
					return false, fmt.Errorf("history entry %d lacks applied digests", i)
				}
				if i > 0 && !bytes.Equal(change.OldDigest.Bytes, view.History[i-1].NewDigest.Bytes) {
					return false, fmt.Errorf("history entry %d breaks digest chain", i)
				}
				if i < int(deltas) {
					if change.OpID == nil || change.Revision != uint64(i+1) || bytes.Equal(change.NewDigest.Bytes, change.OldDigest.Bytes) {
						return false, fmt.Errorf("unexpected delta at %d", i)
					}
				} else if change.OpID != nil || change.Revision != deltas || !bytes.Equal(change.NewDigest.Bytes, change.OldDigest.Bytes) {
					return false, fmt.Errorf("snapshot changed the revision or its digest")
				}
			}
			return true, nil
		})
	case text == "a list walk of 1 session per page reads its first page":
		first, err := flow.sessions.List().Page(1).Results(1).Fetch(ctx)
		if err != nil {
			return err
		}
		if first.TotalResults != 2 || len(first.Items) != 1 {
			return fmt.Errorf("first page has %d of %d sessions", len(first.Items), first.TotalResults)
		}
		flow.firstPage = first
		return nil
	case strings.HasPrefix(text, "the list walk's second page completes "):
		first := flow.firstPage
		next, err := flow.sessions.List().Page(2).Results(1).Bound(laser.SessionBound{AsOf: first.AsOf, Series: first.Series}).Fetch(ctx)
		if err != nil {
			return err
		}
		if next.AsOf != first.AsOf || next.Series != first.Series || next.TotalResults != 2 || len(next.Items) != 1 {
			return fmt.Errorf("second page changed the bound or result count")
		}
		seen := map[wire.ConversationID]bool{first.Items[0].ID: true, next.Items[0].ID: true}
		if len(seen) != 2 || !seen[flow.byLabel[args[0]].Conversation()] || !seen[flow.byLabel[args[1]].Conversation()] {
			return fmt.Errorf("walk returned different sessions")
		}
		return nil
	default:
		return managedSessionLaneStep(ctx, state, flow, text, args)
	}
}

func managedSessionLaneStep(ctx context.Context, state *world, flow *managedSessionFlow, text string, args []string) error {
	owner, err := bridgeAgent("planner")
	if err != nil {
		return err
	}
	lane := state.topic(wire.AgentSessions)
	switch {
	case text == "I retain the session and state handles for lane identity checks":
		flow.retained = flow.session.As(owner)
		flow.retainedState = flow.session.State()
		return nil
	case text == "I retain a generation-bearing reference to its first record":
		ref, err := firstManagedSource(ctx, state, flow)
		flow.retainedSource = ref
		return err
	case strings.HasPrefix(text, "the session source changes its "):
		switch strings.TrimPrefix(text, "the session source changes its ") {
		case "partition count":
			return lane.AddPartitions(ctx, 1)
		case "topic generation":
			if _, err := lane.Delete(ctx); err != nil {
				return err
			}
			return lane.Ensure(ctx, flow.partitions, nil)
		case "stream generation":
			stream := state.client.Stream(state.stream)
			if _, err := stream.Delete(ctx); err != nil {
				return err
			}
			if err := stream.Ensure(ctx); err != nil {
				return err
			}
			return state.client.Bootstrap(ctx, flow.partitions, laser.ExpireAfter(24*time.Hour))
		}
	case text == "retained session handles refuse lifecycle and state writes as stale" || text == "retained session handles still refuse the recovered lane":
		before, err := lane.Get(ctx)
		if err != nil {
			return err
		}
		value, err := dynOf(2)
		if err != nil {
			return err
		}
		calls := []func() error{
			func() error {
				_, err := flow.retained.End(ctx, wire.TaskCanceled, wire.SessionEnd{})
				return err
			},
			func() error { _, err := flow.retainedState.Set(ctx, "after", value); return err },
		}
		if text == "retained session handles refuse lifecycle and state writes as stale" {
			worker, err := bridgeAgent("worker")
			if err != nil {
				return err
			}
			record := wire.RecordID{ID: wire.IDFromBytes(laser.NewOperationID())}
			calls = append(calls,
				func() error {
					_, err := flow.session.End(ctx, wire.TaskCanceled, wire.SessionEnd{})
					return err
				},
				func() error { _, err := flow.retainedState.Snapshot(ctx); return err },
				func() error {
					_, err := flow.session.Append(ctx, wire.AgentEnvelope{
						Kind: wire.KindEvent, Record: &record, Conversation: flow.session.Conversation(),
						Source: owner, Body: []byte{1},
					})
					return err
				},
				func() error {
					_, lease, err := flow.sessions.Start().As(owner).Begin(ctx)
					if lease != nil {
						lease.Release()
					}
					return err
				},
				func() error {
					_, err := flow.sessions.Submit(worker, []byte{1}).From(owner).Send(ctx)
					return err
				},
			)
		}
		for index, call := range calls {
			if err := call(); !errors.As(err, new(*laser.StaleLaneError)) {
				return fmt.Errorf("stale write %d: expected stale lane error, got %v", index, err)
			}
		}
		current, err := lane.Get(ctx)
		if err != nil {
			return err
		}
		if before == nil || current == nil || before.MessagesCount != current.MessagesCount {
			return fmt.Errorf("stale writes changed lane count")
		}
		return nil
	case text == "a fresh handle refuses the stale lane registration":
		before, err := lane.Get(ctx)
		if err != nil {
			return err
		}
		_, err = state.client.SessionsWith(flow.config).Open(flow.session.Conversation(), flow.label).As(owner).
			End(ctx, wire.TaskCanceled, wire.SessionEnd{})
		if !errors.As(err, new(*laser.StaleLaneError)) {
			return fmt.Errorf("fresh handle expected stale registration, got %v", err)
		}
		current, err := lane.Get(ctx)
		if err != nil {
			return err
		}
		if before == nil || current == nil || before.MessagesCount != current.MessagesCount {
			return fmt.Errorf("the refused cancel changed the lane count")
		}
		return nil
	case text == "the session source is explicitly removed and registered again":
		sessions := state.client.SessionsWith(flow.config)
		if err := sessions.Unregister(ctx); err != nil {
			return err
		}
		if err := waitForSessionIndex(ctx, "registration removal", func() (bool, error) {
			_, err := sessions.Sources(ctx, flow.session.Conversation())
			var refusal *laser.SessionError
			if errors.As(err, &refusal) {
				switch refusal.Failure.Kind {
				case wire.SessionNotRegistered:
					return true, nil
				case wire.SessionStale:
					return false, nil
				}
			}
			return false, err
		}); err != nil {
			return err
		}
		sessions = state.client.SessionsWith(flow.config)
		boot, err := sessions.Bootstrap(ctx, flow.partitions, laser.ExpireAfter(24*time.Hour))
		if err != nil {
			return err
		}
		if !boot.Registered {
			return fmt.Errorf("registration was not confirmed")
		}
		flow.sessions = sessions
		return nil
	case text == "a fresh session can write lifecycle and state on the recovered lane":
		fresh := flow.sessions.Start().As(owner)
		_, lease, err := fresh.Begin(ctx)
		if err != nil {
			return err
		}
		defer lease.Release()
		value, err := dynOf(2)
		if err != nil {
			return err
		}
		if _, err = fresh.State().Set(ctx, "after", value); err != nil {
			return err
		}
		_, err = fresh.Complete(ctx, "recovered")
		return err
	case text == "a replacement session records its start at the retained source address":
		fresh := flow.sessions.Open(flow.session.Conversation(), "replacement").As(owner)
		_, lease, err := fresh.Begin(ctx)
		if err != nil {
			return err
		}
		lease.Release()
		flow.session = fresh
		replacement, err := firstManagedSource(ctx, state, flow)
		if err != nil {
			return err
		}
		old, next := flow.retainedSource.Message, replacement.Message
		if next.Stream != old.Stream || next.Topic != old.Topic || next.Partition != old.Partition || next.Offset != old.Offset || *next.Generation == *old.Generation {
			return fmt.Errorf("replacement did not reuse the address with a different generation: %v vs %v", old, next)
		}
		return nil
	case text == "reading the retained source reference returns no replacement record":
		record, err := state.client.ReadAt(ctx, flow.retainedSource)
		if err != nil {
			return err
		}
		if record != nil {
			return fmt.Errorf("stale reference returned a record")
		}
		return nil
	}
	return managedSessionRelations(ctx, state, flow, text, args)
}

func firstManagedSource(ctx context.Context, state *world, flow *managedSessionFlow) (wire.SourceRef, error) {
	var ref wire.SourceRef
	err := waitForSessionIndex(ctx, "first record of the registered generation", func() (bool, error) {
		events, err := flow.sessions.Events(flow.session.Conversation()).Results(1).Fetch(ctx)
		if err != nil || len(events.Items) == 0 {
			return false, err
		}
		ref = events.Items[0].At
		if ref.Message == nil {
			return false, fmt.Errorf("first event has no message reference")
		}
		sources, err := flow.sessions.Sources(ctx, flow.session.Conversation())
		if err != nil || sources.Lane == nil {
			return false, err
		}
		if sources.Lane.TopicID != ref.Message.Topic {
			return false, fmt.Errorf("first event is outside the registered lane")
		}
		ref.Message.Generation = &sources.Lane.Generation
		record, err := state.client.ReadAt(ctx, ref)
		return record != nil, err
	})
	return ref, err
}
