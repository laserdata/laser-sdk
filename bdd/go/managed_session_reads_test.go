package bdd_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func managedSessionReadStep(ctx context.Context, state *world, flow *managedSessionFlow, text string, args []string) error {
	switch {
	case strings.HasPrefix(text, "the session stays quiet for "):
		delay, ok := after(text, "for ")
		if !ok {
			return fmt.Errorf("missing quiet period")
		}
		select {
		case <-time.After(time.Duration(delay) * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case text == "the indexed session is live with a recent heartbeat" || text == "the indexed session becomes idle":
		wantIdle := strings.HasSuffix(text, "becomes idle")
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			info, err := flow.sessions.Get(ctx, flow.session.Conversation())
			if err != nil {
				return false, err
			}
			return info.Status == wire.SessionActive && info.Idle == wantIdle && !info.Flags.LivenessUnknown && info.LastHeartbeatAt != nil && info.LastEventAt != nil && *info.LastHeartbeatAt > *info.LastEventAt, nil
		})
	case text == "no indexed event of the session is a heartbeat":
		events, err := flow.sessions.Events(flow.session.Conversation()).Fetch(ctx)
		if err != nil {
			return err
		}
		if len(events.Items) == 0 {
			return fmt.Errorf("indexed session has no events")
		}
		for _, e := range events.Items {
			if strings.Contains(e.Display, "heartbeat") || strings.Contains(e.Kind, "heartbeat") {
				return fmt.Errorf("heartbeat appears as event %v", e)
			}
		}
		return nil
	case text == "the session lease is released":
		flow.lease.Release()
		return nil
	case text == "no heartbeat of the old lease counts in the recreated stream":
		select {
		case <-time.After(3 * time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
		defer flow.lease.Release()
		stream, err := state.client.Stream(state.stream).Get(ctx)
		if err != nil {
			return err
		}
		cursor, err := state.topic(wire.AgentHeartbeats).Replay()
		if err != nil {
			return err
		}
		for {
			records, err := cursor.Poll(ctx)
			if err != nil {
				return err
			}
			if len(records) == 0 {
				break
			}
			for _, r := range records {
				var envelope wire.AgentEnvelope
				var heartbeat wire.SessionHeartbeat
				if err := wire.Unmarshal(r.Payload(), &envelope); err != nil {
					return err
				}
				if err := wire.Unmarshal(envelope.Body, &heartbeat); err != nil {
					return err
				}
				if heartbeat.CountsFor(stream.CreatedAtMicros) {
					return fmt.Errorf("old lease counts for the new stream generation")
				}
			}
		}
		return nil
	case strings.HasPrefix(text, "the session streams ") && strings.HasSuffix(text, "chunks in one batch"):
		n, ok := after(text, "streams ")
		if !ok {
			return fmt.Errorf("missing chunk count")
		}
		agdx, err := flow.session.Agdx()
		if err != nil {
			return err
		}
		id := flow.session.Conversation().ID
		bodies := make([][]byte, n)
		for i := range bodies {
			bodies[i] = fmt.Appendf(nil, "chunk-%d", i)
		}
		_, err = agdx.SendChunks(ctx, wire.CorrelationID{ID: id}, wire.ChannelID{ID: id}, "chat", 0, bodies)
		return err
	case strings.HasPrefix(text, "the index counts "):
		n, err := numberAfter(text, "counts ")
		if err != nil {
			return err
		}
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			info, err := flow.sessions.Get(ctx, flow.session.Conversation())
			return info != nil && info.Events == n, err
		})
	case strings.HasPrefix(text, "the change feed names the session in at most "):
		limit, err := numberAfter(text, "at most ")
		if err != nil {
			return err
		}
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			page, err := flow.sessions.Changes(ctx, 1, 100)
			if err != nil {
				return false, err
			}
			var count uint64
			for _, row := range page.Items {
				if slices.Contains(row.Sessions, flow.session.Conversation()) {
					count++
				}
			}
			if count > limit {
				return false, fmt.Errorf("fold produced %d rows", count)
			}
			return count > 0, nil
		})
	case text == "the change feed never names the second stream's session":
		other := flow.secondSession.Conversation()
		if err := waitForSessionIndex(ctx, "second stream fold", func() (bool, error) {
			p, err := flow.second.Changes(ctx, 1, 100)
			if err != nil {
				return false, err
			}
			for _, row := range p.Items {
				if slices.Contains(row.Sessions, other) {
					return true, nil
				}
			}
			return false, nil
		}); err != nil {
			return err
		}
		page, err := flow.sessions.Changes(ctx, 1, 100)
		if err != nil {
			return err
		}
		for _, row := range page.Items {
			if slices.Contains(row.Sessions, other) {
				return fmt.Errorf("feed contains a foreign stream's session")
			}
		}
		return nil
	case text == "a watch follows the stream's session changes":
		watch, err := flow.sessions.Watch(ctx, 50*time.Millisecond)
		flow.watch = watch
		return err
	case strings.HasPrefix(text, "the session records a call of tool "):
		payload, err := dynOf(map[string]any{"token": "synthetic-token", "q": "test"})
		if err != nil {
			return err
		}
		if len(args) != 1 {
			return fmt.Errorf("want one tool name, got %v", args)
		}
		call, err := flow.session.Tool(ctx, args[0], payload)
		if err != nil {
			return err
		}
		_, err = call.Complete(ctx, []byte("done"))
		return err
	case text == "the session ends":
		_, err := flow.session.Complete(ctx, "done")
		return err
	case strings.HasPrefix(text, "listing the stream finds "):
		if len(args) != 1 {
			return fmt.Errorf("want one session label, got %v", args)
		}
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			page, err := flow.sessions.List().Status(wire.SessionCompleted).Fetch(ctx)
			if err != nil {
				return false, err
			}
			for _, row := range page.Items {
				if row.Label != nil && *row.Label == args[0] {
					return true, nil
				}
			}
			return false, nil
		})
	case text == "the session list of the stream is not rebuilding":
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			page, err := flow.sessions.List().Fetch(ctx)
			return page != nil && !page.Rebuilding, err
		})
	case strings.HasPrefix(text, "the indexed events of the session are "):
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			page, err := flow.sessions.Events(flow.session.Conversation()).Fetch(ctx)
			if err != nil {
				return false, err
			}
			if len(page.Items) < len(args) {
				return false, nil
			}
			got := make([]string, len(page.Items))
			for i, e := range page.Items {
				got[i] = e.Display
			}
			if !slices.Equal(got, args) {
				return false, fmt.Errorf("want events %v, got %v", args, got)
			}
			return true, nil
		})
	case strings.HasPrefix(text, "the session timeline reads "):
		total, err := numberAfter(text, "reads ")
		if err != nil {
			return err
		}
		pages, err := numberAfter(text, "over ")
		if err != nil {
			return err
		}
		size, err := numberAfter(text, "pages of ")
		if err != nil {
			return err
		}
		seen := map[[4]uint64]bool{}
		for page := uint32(1); uint64(page) <= pages; page++ {
			read, err := flow.sessions.Events(flow.session.Conversation()).Page(page).Results(uint32(size)).Fetch(ctx)
			if err != nil {
				return err
			}
			if read.Page != page || uint64(read.TotalResults) != total || uint64(read.TotalPages) != pages {
				return fmt.Errorf("incorrect timeline pagination")
			}
			for _, e := range read.Items {
				s := e.At.Message
				if s == nil {
					return fmt.Errorf("timeline event has no source")
				}
				seen[[4]uint64{uint64(s.Stream), uint64(s.Topic), uint64(s.Partition), s.Offset}] = true
			}
		}
		if uint64(len(seen)) != total {
			return fmt.Errorf("expected %d distinct timeline entries, got %d", total, len(seen))
		}
		return nil
	case text == "the session sources show folded offsets within their heads":
		sources, err := flow.sessions.Sources(ctx, flow.session.Conversation())
		if err != nil {
			return err
		}
		folded := false
		for _, s := range sources.Sources {
			if s.Folded != nil {
				if s.Head == nil || *s.Folded > *s.Head {
					return fmt.Errorf("invalid folded frontier: %v", s)
				}
				folded = true
			}
		}
		if !folded {
			return fmt.Errorf("no folded source")
		}
		return nil
	case text == "the session has no links":
		view, err := flow.sessions.Links(ctx, flow.session.Conversation(), nil)
		if err != nil {
			return err
		}
		if len(view.Links) > 0 {
			return fmt.Errorf("unexpected links %v", view.Links)
		}
		return nil
	case text == "the watch reports the session":
		bound, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		for {
			change, err := flow.watch.Next(bound)
			if err != nil {
				return err
			}
			if change.Resync {
				return fmt.Errorf("unexpected session watch resync")
			}
			if slices.Contains(change.Sessions, flow.session.Conversation()) {
				return nil
			}
		}
	case strings.HasPrefix(text, "the session records a model call that used "):
		tokens, err := numberAfter(text, "used ")
		if err != nil {
			return err
		}
		_, err = flow.session.RecordModelCall(ctx, laser.ModelRequest{Model: "test", Body: []byte("question")}, laser.ModelResponse{Body: []byte("answer"), Usage: &wire.TokenUsage{InputTokens: tokens}}, nil)
		return err
	case strings.HasPrefix(text, "the indexed session ") && strings.HasSuffix(text, "is over its budget"):
		if len(args) != 1 {
			return fmt.Errorf("want one session label, got %v", args)
		}
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			info, err := flow.sessions.Get(ctx, flow.byLabel[args[0]].Conversation())
			return info != nil && info.OverBudget, err
		})
	case strings.HasPrefix(text, "agent ") && strings.Contains(text, " sends the session "):
		n, err := numberAfter(text, "sends the session ")
		if err != nil {
			return err
		}
		if len(args) < 2 {
			return fmt.Errorf("want a sender and a target, got %v", args)
		}
		target, err := bridgeAgent(args[1])
		if err != nil {
			return err
		}
		agdx, err := flow.session.Agdx()
		if err != nil {
			return err
		}
		for range n {
			id := wire.CorrelationID{ID: wire.IDFromBytes(laser.NewOperationID())}
			if _, err := agdx.Command(id, []byte("work")).WithTarget(target).Send(ctx); err != nil {
				return err
			}
		}
		return nil
	case strings.HasPrefix(text, "agent ") && strings.Contains(text, " handles only the work of the session "):
		if len(args) != 2 {
			return fmt.Errorf("want an agent and a session label, got %v", args)
		}
		want := flow.byLabel[args[1]].Conversation()
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			flow.worker.mu.Lock()
			defer flow.worker.mu.Unlock()
			if len(flow.worker.messages) == 0 {
				return false, nil
			}
			if !slices.Equal(flow.worker.messages, []wire.ConversationID{want}) {
				return false, fmt.Errorf("unexpected work %v", flow.worker.messages)
			}
			return true, nil
		})
	case strings.HasPrefix(text, "the session ") && strings.Contains(text, " ends failed once with reason "):
		if len(args) != 2 {
			return fmt.Errorf("want a session label and a reason, got %v", args)
		}
		id := flow.byLabel[args[0]].Conversation()
		return waitForSessionIndex(ctx, text, func() (bool, error) {
			messages, err := state.client.Context(id).Fetch(ctx, []string{wire.AgentSessions}, 100, 0)
			if err != nil {
				return false, err
			}
			count := 0
			for _, m := range messages {
				e := m.Envelope
				if e == nil || e.Kind != wire.KindStatus || e.TaskState == nil || *e.TaskState != wire.TaskFailed {
					continue
				}
				var end wire.SessionEnd
				if err := wire.Unmarshal(e.Body, &end); err != nil {
					return false, err
				}
				if end.Reason == nil || *end.Reason != args[1] || end.Error == nil || end.Error.Message == nil || !strings.Contains(*end.Error.Message, "budget") {
					return false, fmt.Errorf("unexpected failed session end %v", end)
				}
				count++
			}
			if count > 1 {
				return false, fmt.Errorf("%d failed terminal records", count)
			}
			return count == 1, nil
		})
	}
	return managedWriterStep(ctx, state, flow, text, args)
}

// numberAfter reads the number a step names after phrase, so a reworded step
// fails as a step error and never compares against zero.
func numberAfter(text, phrase string) (uint64, error) {
	value, found := after(text, phrase)
	if !found {
		return 0, fmt.Errorf("the step %q names no number after %q", text, phrase)
	}
	return value, nil
}
