package wire

import (
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// decodeCBORPayload decodes one CBOR data item within the limits.
//
// The bytes are walked once for structure before any value is built, so depth
// and value count are bounded before the decoder allocates. Tags are refused:
// a tag needs an explicit semantic profile, and silently unwrapping one would
// let a producer change what a filter compares against.
func decodeCBORPayload(payload []byte, limits DecodeLimits) (payloadValue, FaultReason) {
	if len(payload) > limits.MaxPayloadBytes {
		return payloadValue{}, FaultTooLarge
	}
	budget := limits.MaxPayloadBytes
	if budget < 1 {
		budget = 1
	}
	scan := cborScan{bytes: payload, maxDepth: limits.parseDepth(), remaining: budget}
	if reason := scan.item(0); reason != "" {
		return payloadValue{}, reason
	}
	if len(scan.bytes) != 0 {
		return payloadValue{}, FaultMalformed
	}

	item, err := cborform.Decode(payload)
	if err != nil {
		return payloadValue{}, FaultMalformed
	}
	value, ok := cborPayloadValue(item)
	if !ok {
		return payloadValue{}, FaultMalformed
	}
	return value, ""
}

// cborScan walks CBOR heads without building values.
type cborScan struct {
	bytes     []byte
	maxDepth  int
	remaining int
}

func (scan *cborScan) take(count int) ([]byte, bool) {
	if count < 0 || count > len(scan.bytes) {
		return nil, false
	}
	head := scan.bytes[:count]
	scan.bytes = scan.bytes[count:]
	return head, true
}

func (scan *cborScan) item(depth int) FaultReason {
	if depth > scan.maxDepth {
		return FaultTooDeep
	}
	if scan.remaining == 0 {
		return FaultTooLarge
	}
	scan.remaining--

	header, ok := scan.take(1)
	if !ok {
		return FaultMalformed
	}
	major, info := header[0]>>5, header[0]&31
	// A tag has no agreed meaning here, and an indefinite length has no
	// canonical encoding, so neither reaches the decoder.
	if major == 6 || info == 31 {
		return FaultMalformed
	}
	argument := uint64(info)
	if info >= 24 {
		if info > 27 {
			return FaultMalformed
		}
		width, ok := scan.take(1 << (info - 24))
		if !ok {
			return FaultMalformed
		}
		argument = 0
		for _, octet := range width {
			argument = argument<<8 | uint64(octet)
		}
	}

	if major == 2 || major == 3 {
		length, fits := intFromU64(argument)
		if !fits {
			return FaultMalformed
		}
		if _, ok := scan.take(length); !ok {
			return FaultMalformed
		}
	}
	if major == 4 || major == 5 {
		count := argument
		if major == 5 {
			if count > math.MaxUint64/2 {
				return FaultMalformed
			}
			count *= 2
		}
		// Each child needs at least one byte, so a count beyond what is left
		// is malformed before any of it is walked.
		if count > uint64(len(scan.bytes)) {
			return FaultMalformed
		}
		for index := uint64(0); index < count; index++ {
			if reason := scan.item(depth + 1); reason != "" {
				return reason
			}
		}
	}
	return ""
}

func intFromU64(value uint64) (int, bool) {
	if value > uint64(math.MaxInt) {
		return 0, false
	}
	return int(value), true
}

// cborPayloadValue converts a decoded CBOR item into the shared payload model.
func cborPayloadValue(item cborform.Value) (payloadValue, bool) {
	switch typed := item.(type) {
	case cborform.Bool:
		return payloadValue{kind: payloadBool, flag: bool(typed)}, true
	case cborform.Null, cborform.Undefined:
		return payloadValue{kind: payloadNull}, true
	case cborform.Uint:
		return payloadValue{
			kind: payloadNumber, number: strconv.FormatUint(uint64(typed), 10),
		}, true
	case cborform.NegInt:
		// The wire holds -1 - n, so a magnitude past the signed range has no
		// 64-bit integer to decode into.
		if uint64(typed) >= 1<<63 {
			return payloadValue{}, false
		}
		return payloadValue{
			kind: payloadNumber, number: strconv.FormatInt(-1-int64(typed), 10),
		}, true
	case cborform.Float:
		if math.IsInf(typed.Value, 0) || math.IsNaN(typed.Value) {
			return payloadValue{}, false
		}
		return payloadValue{
			kind:    payloadNumber,
			number:  strconv.FormatFloat(typed.Value, 'g', -1, 64),
			isFloat: true,
		}, true
	case cborform.Text:
		if !utf8.ValidString(string(typed)) {
			return payloadValue{}, false
		}
		return payloadValue{kind: payloadText, text: string(typed)}, true
	case cborform.Bytes:
		// A byte string compares as its octets, so a filter addresses one with
		// an index just as it would any other array.
		items := make([]payloadValue, len(typed))
		for index, octet := range typed {
			items[index] = payloadValue{
				kind: payloadNumber, number: strconv.FormatUint(uint64(octet), 10),
			}
		}
		return payloadValue{kind: payloadArray, array: items}, true
	case cborform.Array:
		items := make([]payloadValue, len(typed))
		for index, element := range typed {
			value, ok := cborPayloadValue(element)
			if !ok {
				return payloadValue{}, false
			}
			items[index] = value
		}
		return payloadValue{kind: payloadArray, array: items}, true
	case cborform.Map:
		fields := make(map[string]payloadValue, len(typed))
		for _, entry := range typed {
			// A non-text key cannot be addressed by a field path, and a
			// repeated key leaves which value a filter reads undefined, so
			// neither decodes.
			key, ok := entry.Key.(cborform.Text)
			if !ok || !utf8.ValidString(string(key)) {
				return payloadValue{}, false
			}
			if _, repeated := fields[string(key)]; repeated {
				return payloadValue{}, false
			}
			value, ok := cborPayloadValue(entry.Value)
			if !ok {
				return payloadValue{}, false
			}
			fields[string(key)] = value
		}
		return payloadValue{kind: payloadObject, object: fields}, true
	default:
		// A tag or a simple value with no JSON meaning never decodes.
		return payloadValue{}, false
	}
}
