package examples

import (
	"fmt"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// SessionRetention is how long agent.sessions keeps records in the examples.
const SessionRetention = 24 * time.Hour

// AgentIDs parses the agent names an example uses, in order.
func AgentIDs(names ...string) ([]wire.AgentID, error) {
	ids := make([]wire.AgentID, 0, len(names))
	for _, name := range names {
		id, err := wire.NewAgentID(name)
		if err != nil {
			return nil, fmt.Errorf("agent id %q: %w", name, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}
