package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// OpsStream is the stream carrying the ops topics.
func (laser *Laser) OpsStream() string {
	if laser.topology.opsStream != "" {
		return laser.topology.opsStream
	}
	return laser.announcedTopology().OpsStream
}

// ControlTopic is the control command topic on the ops stream.
func (laser *Laser) ControlTopic() string {
	if laser.topology.controlTopic != "" {
		return laser.topology.controlTopic
	}
	return laser.announcedTopology().ControlTopic
}

// DLQTopic is the dead-letter topic on the ops stream.
func (laser *Laser) DLQTopic() string {
	if laser.topology.dlqTopic != "" {
		return laser.topology.dlqTopic
	}
	return laser.announcedTopology().DLQTopic
}

// ChangesTopic is the change feed topic on the ops stream.
func (laser *Laser) ChangesTopic() string {
	if laser.topology.changesTopic != "" {
		return laser.topology.changesTopic
	}
	return laser.announcedTopology().ChangesTopic
}

// WithOpsStream derives a handle whose ops topics ride another stream. An empty name restores the announced one.
func (laser *Laser) WithOpsStream(stream string) *Laser {
	derived := laser.copy()
	derived.topology.opsStream = stream
	return derived
}

// WithControlTopic derives a handle on another control command topic. An empty name restores the announced one.
func (laser *Laser) WithControlTopic(topic string) *Laser {
	derived := laser.copy()
	derived.topology.controlTopic = topic
	return derived
}

// WithDLQTopic derives a handle on another dead-letter topic. An empty name restores the announced one.
func (laser *Laser) WithDLQTopic(topic string) *Laser {
	derived := laser.copy()
	derived.topology.dlqTopic = topic
	return derived
}

// WithChangesTopic derives a handle on another change feed topic. An empty name restores the announced one.
func (laser *Laser) WithChangesTopic(topic string) *Laser {
	derived := laser.copy()
	derived.topology.changesTopic = topic
	return derived
}

// topologyOverrides are the ops names one handle pins, each empty when it follows the deployment.
type topologyOverrides struct {
	opsStream, controlTopic, dlqTopic, changesTopic string
}

// topology is the ops naming these options pin.
func (options Options) topology() topologyOverrides {
	return topologyOverrides{
		opsStream:    options.OpsStream,
		controlTopic: options.ControlTopic,
		dlqTopic:     options.DLQTopic,
		changesTopic: options.ChangesTopic,
	}
}

// announcedTopology is the ops naming the deployment last announced, the defaults when it announced none.
func (laser *Laser) announcedTopology() wire.WireTopology {
	shared := laser.state()
	shared.mutex.RLock()
	defer shared.mutex.RUnlock()
	if shared.topology != nil {
		return *shared.topology
	}
	return wire.DefaultWireTopology()
}

// adoptTopology keeps a newly announced topology. A probe that announced none leaves the last one.
func (laser *Laser) adoptTopology(announced *wire.WireTopology) {
	if announced == nil {
		return
	}
	shared := laser.state()
	shared.mutex.Lock()
	defer shared.mutex.Unlock()
	shared.topology = announced
}

// negotiateTopology is NegotiateCapabilities that also returns the topology the deployment announced.
func negotiateTopology(
	ctx context.Context, client BinaryRequester, configured Capabilities,
) (Capabilities, *wire.WireTopology) {
	negotiated := configured
	announce, err := probeManagedHost(ctx, client)
	switch {
	case err != nil:
		negotiated.Hello = HelloFailed
	case announce == nil:
		negotiated.Hello = HelloRejected
	default:
		negotiated.mergeAnnouncement(announce)
		negotiated.Hello = HelloAnswered
		return negotiated, announce.Topology
	}
	return negotiated, nil
}
