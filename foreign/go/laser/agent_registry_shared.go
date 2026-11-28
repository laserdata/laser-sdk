package laser

import (
	"context"
	"sync"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// sharedRegistry is one stream's registry and the cursor its card fold
// resumes from.
type sharedRegistry struct {
	mutex    sync.Mutex
	registry *AgentRegistry
	cursor   *Cursor
}

// agentRegistry is the shared registry of the default stream, folded up to the
// tail of the registry topic.
func (laser *Laser) agentRegistry(ctx context.Context) (*AgentRegistry, error) {
	held := laser.state()
	held.mutex.Lock()
	shared, opened := held.registries[laser.defaultStream]
	if !opened {
		if held.registries == nil {
			held.registries = map[string]*sharedRegistry{}
		}
		shared = &sharedRegistry{registry: NewAgentRegistry()}
		held.registries[laser.defaultStream] = shared
	}
	held.mutex.Unlock()
	shared.mutex.Lock()
	defer shared.mutex.Unlock()
	if shared.cursor == nil {
		cursor, err := laser.Topic(wire.AgentRegistry).Replay()
		if err != nil {
			return nil, err
		}
		shared.cursor = cursor
	}
	for {
		read, err := shared.registry.refresh(ctx, shared.cursor)
		if err != nil {
			return nil, err
		}
		// One poll is capped per partition, so a full one may have left cards unread.
		if read < MaxDrainRecords {
			return shared.registry, nil
		}
	}
}
