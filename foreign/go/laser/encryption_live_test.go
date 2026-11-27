//go:build integration

// Live coverage for client-side encryption.
//
// The hermetic suite pins ciphertexts that the Rust and Node encryptors
// sealed, which proves the format. What it cannot prove is that an encrypted
// record survives a server: a deployment that rejected, padded or re-framed a
// sealed payload would leave every one of those vectors passing.
//
// So these tests put sealed bytes on a real log and read them back, and they
// assert that what the server stored is NOT the plaintext. That last check is
// the point of client-side encryption and the one thing no fixture can show.
package laser_test

import (
	"bytes"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// liveEncryptionKey is the 32-byte key these tests seal under.
//
// It is fixed rather than random so a failure is reproducible, and it is a
// test key in a test file: nothing it protects outlives the run.
var liveEncryptionKey = bytes.Repeat([]byte{7}, 32)

// sealedHeaderKey is the header the sealed header block rides under.
//
// The whole block is sealed as one opaque value rather than key by key,
// because encrypting values alone would leave the key names in the clear and
// the names are often as telling as the values.
const sealedHeaderKey = "x-sealed-headers"

// TestGivenEncryptedRecordWhenConsumedThenShouldDecryptPayloadAndHeaders
// publishes a sealed payload and a sealed header block, reads them back off
// the server, and opens both.
func TestGivenEncryptedRecordWhenConsumedThenShouldDecryptPayloadAndHeaders(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)
	topic := liveTopic(t, client, ctx)
	consumer := liveConsumer(t, topic, ctx, livePartitions(t, topic, ctx))

	encryption, err := laser.AES256GCM(liveEncryptionKey)
	if err != nil {
		t.Fatalf("build the encryption: %v", err)
	}

	plaintext := []byte("the body nobody but the holder of the key may read")
	sealedPayload, err := encryption.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("seal the payload: %v", err)
	}

	// The header block is serialized first and then sealed whole, which is the
	// same shape Apache Iggy's own encryptor writes.
	headerBlock := iggcon.GetHeadersBytes([]iggcon.HeaderEntry{
		sealedStringHeader(t, "tenant", "acme"),
		sealedStringHeader(t, "subject", "payroll run"),
	})
	sealedHeaders, err := encryption.Encrypt(headerBlock)
	if err != nil {
		t.Fatalf("seal the header block: %v", err)
	}

	carrier := iggcon.HeaderEntry{
		Key: sealedHeaderKeyEntry(t, sealedHeaderKey),
		// Raw, because a sealed block is opaque bytes and declaring it a string
		// would invite a reader to render bytes that are not text.
		Value: iggcon.HeaderValue{Kind: iggcon.Raw, Value: sealedHeaders},
	}
	if _, err := topic.Send(ctx, sealedPayload,
		[]iggcon.HeaderEntry{carrier}, "sealed"); err != nil {
		t.Fatalf("publish the sealed record: %v", err)
	}

	record, err := consumer.Next(ctx)
	if err != nil {
		t.Fatalf("read the sealed record back: %v", err)
	}

	// This is what only a live test can establish: the server holds ciphertext.
	// If the plaintext were recoverable from what came back without the key,
	// client-side encryption would be doing nothing at all.
	if bytes.Contains(record.Payload(), plaintext) {
		t.Fatal("the server returned the plaintext: the record was not sealed on the wire")
	}
	if !bytes.Equal(record.Payload(), sealedPayload) {
		t.Fatalf("the sealed payload changed in transit: sent %d bytes, read %d",
			len(sealedPayload), len(record.Payload()))
	}

	opened, err := encryption.Decrypt(record.Payload())
	if err != nil {
		t.Fatalf("open the payload that was just read back: %v", err)
	}
	if !bytes.Equal(opened, plaintext) {
		t.Fatalf("the opened payload is not what was sealed: sent %q, opened %q",
			plaintext, opened)
	}

	entries, err := iggcon.DeserializeHeaders(record.Message.UserHeaders)
	if err != nil {
		t.Fatalf("parse the record's user headers: %v", err)
	}
	var carried []byte
	for _, entry := range entries {
		if string(entry.Key.Value) == sealedHeaderKey {
			carried = entry.Value.Value
		}
	}
	if carried == nil {
		t.Fatalf("the sealed header block is missing from the %d headers read back",
			len(entries))
	}
	if bytes.Contains(carried, []byte("payroll run")) {
		t.Fatal("the header block came back in the clear: it was not sealed on the wire")
	}

	openedHeaders, err := encryption.Decrypt(carried)
	if err != nil {
		t.Fatalf("open the header block that was just read back: %v", err)
	}
	reopened, err := iggcon.DeserializeHeaders(openedHeaders)
	if err != nil {
		t.Fatalf("parse the opened header block: %v", err)
	}
	if got := sealedHeaderText(reopened, "subject"); got != "payroll run" {
		t.Errorf("the opened header block lost a value: subject read %q", got)
	}
	if got := sealedHeaderText(reopened, "tenant"); got != "acme" {
		t.Errorf("the opened header block lost a value: tenant read %q", got)
	}
}

// TestGivenRecordSealedUnderAnotherKeyWhenDecryptedThenShouldRefuseIt proves
// the live failure path: a holder of the wrong key gets an error rather than
// anything it might mistake for the record.
func TestGivenRecordSealedUnderAnotherKeyWhenDecryptedThenShouldRefuseIt(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)
	topic := liveTopic(t, client, ctx)
	consumer := liveConsumer(t, topic, ctx, livePartitions(t, topic, ctx))

	sealing, err := laser.AES256GCM(liveEncryptionKey)
	if err != nil {
		t.Fatalf("build the sealing encryption: %v", err)
	}
	// A key that differs in one byte, because a key that differs everywhere
	// would also be refused by a check far weaker than the tag.
	otherKey := bytes.Repeat([]byte{7}, 32)
	otherKey[31] = 8
	opening, err := laser.AES256GCM(otherKey)
	if err != nil {
		t.Fatalf("build the opening encryption: %v", err)
	}

	sealed, err := sealing.Encrypt([]byte("not for this reader"))
	if err != nil {
		t.Fatalf("seal the payload: %v", err)
	}
	if _, err := topic.Publish().Payload(sealed).Key("sealed").Send(ctx); err != nil {
		t.Fatalf("publish the sealed record: %v", err)
	}

	record, err := consumer.Next(ctx)
	if err != nil {
		t.Fatalf("read the sealed record back: %v", err)
	}
	if _, err := opening.Decrypt(record.Payload()); err == nil {
		t.Fatal("a record sealed under another key was opened: the tag is not being checked")
	}
}

// sealedStringHeader builds one string user header.
func sealedStringHeader(t *testing.T, name, value string) iggcon.HeaderEntry {
	t.Helper()
	return iggcon.HeaderEntry{
		Key:   sealedHeaderKeyEntry(t, name),
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
	}
}

// sealedHeaderKeyEntry builds one header key.
func sealedHeaderKeyEntry(t *testing.T, name string) iggcon.HeaderKey {
	t.Helper()
	key, err := iggcon.NewHeaderKeyString(name)
	if err != nil {
		t.Fatalf("build the header key %s: %v", name, err)
	}
	return key
}

// sealedHeaderText reads one string header value out of a parsed block.
func sealedHeaderText(entries []iggcon.HeaderEntry, name string) string {
	for _, entry := range entries {
		if string(entry.Key.Value) == name {
			return string(entry.Value.Value)
		}
	}
	return ""
}
