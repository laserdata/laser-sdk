package laser

import "context"

// SetCoordinationTransportForTest runs this handle's lease acquisitions over transport, standing in for the dedicated connection a connection string opens.
func SetCoordinationTransportForTest(laser *Laser, transport ManagedKvTransport) *Laser {
	shared := laser.state()
	shared.mutex.Lock()
	defer shared.mutex.Unlock()
	shared.coordination = NewFencedLeaseClient(transport)
	return laser
}

// CoordinateOverClientForTest runs this handle's lease acquisitions over its own client, so a fake server answers them.
func CoordinateOverClientForTest(laser *Laser) *Laser {
	return SetCoordinationTransportForTest(laser, clientKvTransport{laser: laser})
}

type clientKvTransport struct{ laser *Laser }

func (transport clientKvTransport) Send(ctx context.Context, code uint32, frame []byte) ([]byte, error) {
	return sendCoordinationFrame(ctx, transport.laser, code, frame)
}

func (clientKvTransport) Reset() {}
