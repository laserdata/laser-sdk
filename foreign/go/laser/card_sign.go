package laser

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// AgentCardSignature is a detached JWS over an A2A discovery card.
type AgentCardSignature struct {
	Protected string `json:"protected"`
	Signature string `json:"signature"`
}

// SignCardValue signs every top-level card member except signatures.
// Integers must be in the exact IEEE 754 range supported by the SDKs.
func SignCardValue(key *SigningKey, card json.RawMessage) (*AgentCardSignature, error) {
	if key == nil || key.private == nil {
		return nil, &SignatureError{Reason: "a card needs an Ed25519 signing key"}
	}
	header := fmt.Sprintf(`{"alg":"EdDSA","kid":"%x"}`, key.keyID)
	protected := base64.RawURLEncoding.EncodeToString([]byte(header))
	input, err := cardSigningInput(card, protected)
	if err != nil {
		return nil, err
	}
	return &AgentCardSignature{Protected: protected,
		Signature: base64.RawURLEncoding.EncodeToString(ed25519.Sign(key.private(), input))}, nil
}

// VerifyCard verifies a detached signature against an enrolled public key.
// Pass the original card JSON so extension fields remain covered.
func VerifyCard(card json.RawMessage, signature AgentCardSignature, verifying ed25519.PublicKey) error {
	if len(verifying) != ed25519.PublicKeySize {
		return &SignatureError{Reason: "invalid Ed25519 verifying key"}
	}
	if strings.ContainsAny(signature.Protected, "\r\n") || strings.ContainsAny(signature.Signature, "\r\n") {
		return &SignatureError{Reason: "invalid base64url in card signature"}
	}
	header, err := base64.RawURLEncoding.Strict().DecodeString(signature.Protected)
	if err != nil || !json.Valid(header) {
		return &SignatureError{Reason: "invalid card signature protected header"}
	}
	fields, err := cardObject(header)
	if err != nil {
		return err
	}
	if alg, ok := fields["alg"].(string); !ok || alg != "EdDSA" {
		return &SignatureError{Reason: "card signature requires EdDSA"}
	}
	if _, critical := fields["crit"]; critical {
		return &SignatureError{Reason: "unknown critical card signature extension"}
	}
	if kid, present := fields["kid"]; present && kid != hex.EncodeToString(KeyIDOf(verifying)) {
		return &SignatureError{Reason: "card signature names another key"}
	}
	input, err := cardSigningInput(card, signature.Protected)
	if err != nil {
		return err
	}
	sig, err := base64.RawURLEncoding.Strict().DecodeString(signature.Signature)
	if err != nil || !ed25519.Verify(verifying, input, sig) {
		return &SignatureError{Reason: "card signature does not verify"}
	}
	return nil
}

func cardSigningInput(card []byte, protected string) ([]byte, error) {
	fields, err := cardObject(card)
	if err != nil {
		return nil, err
	}
	delete(fields, "signatures")
	var payload strings.Builder
	if err := appendCardJSON(&payload, fields); err != nil {
		return nil, err
	}
	return []byte(protected + "." + base64.RawURLEncoding.EncodeToString([]byte(payload.String()))), nil
}

func cardObject(raw []byte) (map[string]any, error) {
	if !json.Valid(raw) || !utf8.Valid(raw) || !pairedJSONSurrogates(raw) {
		return nil, &SignatureError{Reason: "invalid card JSON"}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := readCardValue(decoder)
	if err != nil {
		return nil, err
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return nil, &SignatureError{Reason: "card JSON must be an object"}
	}
	return fields, nil
}

// encoding/json replaces lone surrogate escapes with U+FFFD. Signing that
// replacement would give malformed input the same signature as valid text.
// raw has already passed json.Valid, including every escape's hex digits.
func pairedJSONSurrogates(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if raw[i] != 'u' {
			continue
		}
		code, _ := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		i += 4
		if code < 0xd800 || code > 0xdfff {
			continue
		}
		if code > 0xdbff || len(raw)-i <= 6 || raw[i+1] != '\\' || raw[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}

func readCardValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	switch token {
	case json.Delim('{'):
		object := make(map[string]any)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name := key.(string)
			if _, duplicate := object[name]; duplicate {
				return nil, &SignatureError{Reason: "duplicate member in card JSON"}
			}
			object[name], err = readCardValue(decoder)
			if err != nil {
				return nil, err
			}
		}
		_, err := decoder.Token()
		return object, err
	case json.Delim('['):
		var array []any
		for decoder.More() {
			value, err := readCardValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, value)
		}
		_, err := decoder.Token()
		return array, err
	default:
		return token, nil
	}
}

func appendCardJSON(out *strings.Builder, value any) error {
	switch v := value.(type) {
	case map[string]any:
		keys := slices.SortedFunc(maps.Keys(v), func(a, b string) int {
			return slices.Compare(utf16.Encode([]rune(a)), utf16.Encode([]rune(b)))
		})
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			writeCardString(out, key)
			out.WriteByte(':')
			if err := appendCardJSON(out, v[key]); err != nil {
				return err
			}
		}
		out.WriteByte('}')
	case []any:
		out.WriteByte('[')
		for i, item := range v {
			if i > 0 {
				out.WriteByte(',')
			}
			if err := appendCardJSON(out, item); err != nil {
				return err
			}
		}
		out.WriteByte(']')
	case string:
		writeCardString(out, v)
	case json.Number:
		n, err := v.Int64()
		const maxInteger = 1<<53 - 1
		// A negative zero is a float to the other clients, which refuse it.
		if err != nil || v == "-0" || n < -maxInteger || n > maxInteger {
			return &SignatureError{Reason: "card numbers must be integers up to 2^53 - 1 in magnitude"}
		}
		out.WriteString(strconv.FormatInt(n, 10))
	case bool:
		out.WriteString(strconv.FormatBool(v))
	case nil:
		out.WriteString("null")
	}
	return nil
}

func writeCardString(out *strings.Builder, text string) {
	out.WriteByte('"')
	for _, r := range text {
		switch r {
		case '"', '\\':
			out.WriteByte('\\')
			out.WriteRune(r)
		case '\b':
			out.WriteString(`\b`)
		case '\t':
			out.WriteString(`\t`)
		case '\n':
			out.WriteString(`\n`)
		case '\f':
			out.WriteString(`\f`)
		case '\r':
			out.WriteString(`\r`)
		default:
			if r < 0x20 {
				fmt.Fprintf(out, `\u%04x`, r)
			} else {
				out.WriteRune(r)
			}
		}
	}
	out.WriteByte('"')
}
