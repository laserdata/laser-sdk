package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ParkedRecords is the work a session's agents held while it was paused and
// have not reported handled.
//
// A session canceled while paused keeps its held records here unprocessed:
// the work was never run, and saying so is the point.
type ParkedRecords struct {
	// Records is the held work, oldest source first.
	Records []wire.SessionParking
	// Complete is false when a held record can no longer be read back
	// because its source expired or its topic was recreated, so what is
	// listed may not be all there was.
	Complete bool
}

// Parked is the work this session's agents held while it was paused and have
// not reported handled.
//
// It is folded from the session's own lane: a parked record adds held work
// and an unparked one takes it away again, so what is left is what is still
// being held. A role that parked a record and then never came back leaves it
// listed, which is exactly the state an operator needs to see.
func (session *Session) Parked(ctx context.Context) (*ParkedRecords, error) {
	envelopes, err := session.laneEnvelopes(ctx)
	if err != nil {
		return nil, err
	}
	return foldParked(envelopes), nil
}

// foldParked reduces a session's lane to the work still held.
//
// Pure, so the fold is testable without a log, and shared by any reader that
// already has the envelopes.
func foldParked(envelopes []wire.AgentEnvelope) *ParkedRecords {
	held := map[laneParkingKey]wire.SessionParking{}
	order := make([]laneParkingKey, 0, len(envelopes))
	complete := true
	for index := range envelopes {
		envelope := &envelopes[index]
		if envelope.Operation == nil {
			continue
		}
		operation := *envelope.Operation
		if operation != wire.OperationSessionParked &&
			operation != wire.OperationSessionUnparked {
			continue
		}
		var parking wire.SessionParking
		if err := wire.Unmarshal(envelope.Body, &parking); err != nil {
			// A record naming itself a parking that will not decode is a
			// hole in the account rather than something to skip quietly.
			complete = false
			continue
		}
		if err := parking.Validate(); err != nil {
			complete = false
			continue
		}
		key := laneParkingKeyOf(parking)
		switch operation {
		case wire.OperationSessionParked:
			if _, already := held[key]; !already {
				order = append(order, key)
			}
			held[key] = parking
		case wire.OperationSessionUnparked:
			delete(held, key)
		}
	}
	records := make([]wire.SessionParking, 0, len(held))
	for _, key := range order {
		if parking, still := held[key]; still {
			records = append(records, parking)
		}
	}
	return &ParkedRecords{Records: records, Complete: complete}
}

// laneParkingKey is the held-work identity a lane fold uses: the source
// position and the role together.
//
// The role is part of the key here, unlike the runtime's own parkingKey,
// because a lane carries every role's parkings: two roles holding the same
// record are two pieces of held work, and one role's resume must not clear
// the other's. A runtime only ever folds its own.
type laneParkingKey struct {
	role   string
	source parkingKey
}

// laneParkingKeyOf is the held-work identity a parking record names.
func laneParkingKeyOf(parking wire.SessionParking) laneParkingKey {
	key := laneParkingKey{role: parking.Role.String()}
	if parking.Source.Message != nil {
		key.source = parkingKey{
			topic:     parking.Source.Message.Topic,
			partition: parking.Source.Message.Partition,
			offset:    parking.Source.Message.Offset,
		}
	}
	return key
}

// parkingKeyOf is the held-work identity one role's own parking names,
// without the role, which that role's own state does not need.
func parkingKeyOf(parking wire.SessionParking) parkingKey {
	return laneParkingKeyOf(parking).source
}
