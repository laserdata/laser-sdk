package laser_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenSharedCardWhenSignedThenShouldMatchRustAndVerifyAllMembers(t *testing.T) {
	data, err := os.ReadFile("../../../sdk/tests/fixtures/card_signature.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Seed      string `json:"secret_seed_hex"`
		Card      json.RawMessage
		Canonical string
		Protected string
		Signature string
	}
	if err := json.Unmarshal(data, &vector); err != nil {
		t.Fatal(err)
	}
	seed, err := hex.DecodeString(vector.Seed)
	if err != nil {
		t.Fatal(err)
	}
	key, err := laser.SigningKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := laser.SignCardValue(key, vector.Card)
	if err != nil {
		t.Fatal(err)
	}
	if sig.Protected != vector.Protected || sig.Signature != vector.Signature {
		t.Fatalf("signature differs from the shared vector: %+v", sig)
	}
	if err := laser.VerifyCard(vector.Card, *sig, key.VerifyingKey()); err != nil {
		t.Fatal(err)
	}
	if err := laser.VerifyCard(bytes.Replace(vector.Card, []byte("Emoji:"), []byte("Icon:"), 1), *sig, key.VerifyingKey()); err == nil {
		t.Fatal("verified a changed extension field")
	}
	for _, header := range []string{`{"alg":"none"}`, `{"alg":"EdDSA","crit":null}`, `{"alg":"EdDSA","kid":"other"}`} {
		modified := *sig
		modified.Protected = base64.RawURLEncoding.EncodeToString([]byte(header))
		input := modified.Protected + "." + base64.RawURLEncoding.EncodeToString([]byte(vector.Canonical))
		modified.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(ed25519.NewKeyFromSeed(seed), []byte(input)))
		if err := laser.VerifyCard(vector.Card, modified, key.VerifyingKey()); err == nil {
			t.Fatal("accepted invalid protected header", header)
		}
	}
	for _, malformed := range []string{`{"a":1,"a":1}`, `{"limit":9007199254740992}`, `{"name":"\uD800"}`, `{"name":"\uDC00"}`} {
		if _, err := laser.SignCardValue(key, []byte(malformed)); err == nil {
			t.Fatal("signed invalid card", malformed)
		}
	}
}

// A negative zero is refused, as the Rust canonicalization refuses it, so one
// card never verifies in one client and fails in another.
func TestGivenACardWithNegativeZeroWhenSignedThenShouldRefuseIt(t *testing.T) {
	key, err := laser.GenerateSigningKey()
	if err != nil {
		t.Fatal(err)
	}
	for _, card := range []string{`{"limit":-0}`, `{"nested":{"limits":[1,-0]}}`} {
		if _, err := laser.SignCardValue(key, []byte(card)); err == nil {
			t.Fatalf("signed a card holding a negative zero: %s", card)
		}
	}
	if _, err := laser.SignCardValue(key, []byte(`{"limit":0,"floor":-1}`)); err != nil {
		t.Fatalf("refused a card of plain integers: %v", err)
	}
}
