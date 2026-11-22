package laser

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/apache/iggy/foreign/go/client/reader"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

var encryptionKey = []byte("laser-encryption-test-key-32byte")

var encryptionPlaintext = []byte(`{"host":"node-7","cpu":82}`)

// Sealed with the same key by Apache Iggy's Rust encryptor and by Node's
// AES-256-GCM, pinned from the Rust and TypeScript suites. They are what
// proves a Go consumer reads what the other clients produced, which no
// round trip through this client alone can show.
const (
	iggyCiphertext = "2cf9899e78c2d7e11900d208ccc590f5ca112bc7f67b1d5180fba3d30354613fa3c954477cc01d73545c7105dce1ac30680fe7a79699"
	nodeCiphertext = "bfafee07d0742f433ed130c340127691377c4f54776eda6e40a841af10d5b4c950612150a76700ff3b017a802971514632695045f015"
)

func testEncryption(t *testing.T) *Encryption {
	t.Helper()
	encryption, err := AES256GCM(encryptionKey)
	if err != nil {
		t.Fatalf("a 32-byte key: %v", err)
	}
	return encryption
}

func TestGivenAKeyWhenEncryptingThenShouldDecryptToThePlaintext(t *testing.T) {
	sealed, err := testEncryption(t).Encrypt(encryptionPlaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if want := gcmNonceBytes + len(encryptionPlaintext) + gcmTagBytes; len(sealed) != want {
		t.Fatalf("sealed %d bytes, want %d", len(sealed), want)
	}
	opened, err := testEncryption(t).Decrypt(sealed)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(opened, encryptionPlaintext) {
		t.Fatalf("opened %q, want %q", opened, encryptionPlaintext)
	}
}

func TestGivenOnePlaintextWhenEncryptedTwiceThenShouldUseAFreshNonce(t *testing.T) {
	encryption := testEncryption(t)
	first, err := encryption.Encrypt(encryptionPlaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	second, err := encryption.Encrypt(encryptionPlaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if bytes.Equal(first, second) {
		t.Fatal("one plaintext sealed twice must not repeat, a reused nonce breaks GCM")
	}
}

func TestGivenTheSharedVectorsWhenDecryptingThenShouldReadThePlaintext(t *testing.T) {
	for name, vector := range map[string]string{
		"the Rust client": iggyCiphertext,
		"the Node client": nodeCiphertext,
	} {
		sealed, err := hex.DecodeString(vector)
		if err != nil {
			t.Fatalf("%s vector: %v", name, err)
		}
		opened, err := testEncryption(t).Decrypt(sealed)
		if err != nil {
			t.Fatalf("decrypt %s vector: %v", name, err)
		}
		if !bytes.Equal(opened, encryptionPlaintext) {
			t.Fatalf("%s vector opened %q, want %q", name, opened, encryptionPlaintext)
		}
	}
}

func TestGivenAnotherKeyWhenDecryptingThenShouldReportIggyError72(t *testing.T) {
	sealed, err := testEncryption(t).Encrypt(encryptionPlaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	other, err := AES256GCM(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatalf("a 32-byte key: %v", err)
	}
	if _, err := other.Decrypt(sealed); !errors.Is(err, ierror.CannotDecryptData{}) {
		t.Fatalf("another key decrypted, or reported %v", err)
	}
}

func TestGivenAShortCiphertextWhenDecryptingThenShouldReportIggyError72(t *testing.T) {
	// A ciphertext shorter than the nonce would slice out of range rather
	// than report anything, so the lengths below the nonce matter as much as
	// the one just short of a whole record.
	for _, size := range []int{0, 1, gcmNonceBytes - 1, gcmNonceBytes,
		gcmNonceBytes + gcmTagBytes - 1} {
		short := make([]byte, size)
		if _, err := testEncryption(t).Decrypt(short); !errors.Is(err, ierror.CannotDecryptData{}) {
			t.Fatalf("a %d-byte ciphertext reported %v", size, err)
		}
	}
}

func TestGivenAnAlteredCiphertextWhenDecryptingThenShouldReportIggyError72(t *testing.T) {
	sealed, err := testEncryption(t).Encrypt(encryptionPlaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	sealed[len(sealed)-1] ^= 0xff
	if _, err := testEncryption(t).Decrypt(sealed); !errors.Is(err, ierror.CannotDecryptData{}) {
		t.Fatalf("an altered ciphertext reported %v", err)
	}
}

func TestGivenAKeyThatIsNotThirtyTwoBytesWhenBuildingThenShouldRefuseIt(t *testing.T) {
	for _, size := range []int{0, 16, 31, 33, 64} {
		encryption, err := AES256GCM(make([]byte, size))
		if err == nil {
			t.Fatalf("a %d-byte key was accepted", size)
		}
		if encryption != nil {
			t.Fatalf("a refused key returned an encryption")
		}
		var invalid *InvalidError
		if !errors.As(err, &invalid) {
			t.Fatalf("a %d-byte key reported %T, want *InvalidError", size, err)
		}
	}
}

func TestGivenAnEncryptionWhenPrintedThenShouldNotShowTheKey(t *testing.T) {
	printed := fmt.Sprintf("%v", testEncryption(t))
	if printed != "Encryption(Aes256Gcm)" {
		t.Fatalf("printed %q, want %q", printed, "Encryption(Aes256Gcm)")
	}
	if bytes.Contains([]byte(printed), encryptionKey) {
		t.Fatal("printing an encryption leaked its key")
	}
}

// sealingServer keeps every record that reached the log, so a test can read
// what the server would have stored.
type sealingServer struct {
	iggcon.Client
	guard    sync.Mutex
	messages []iggcon.IggyMessage
}

func (server *sealingServer) Close() error { return nil }

func (server *sealingServer) SendBinaryRequest(
	_ context.Context, _ uint32, _ []byte,
) ([]byte, error) {
	return nil, ierror.InvalidCommand{}
}

func (server *sealingServer) SendMessages(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier,
	_ iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.messages = append(server.messages, messages...)
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *sealingServer) stored() []iggcon.IggyMessage {
	server.guard.Lock()
	defer server.guard.Unlock()
	return append([]iggcon.IggyMessage(nil), server.messages...)
}

// stringHeader is one string header entry.
func stringHeader(t *testing.T, name, value string) iggcon.HeaderEntry {
	t.Helper()
	key, err := iggcon.NewHeaderKeyString(name)
	if err != nil {
		t.Fatalf("build the header key: %v", err)
	}
	return iggcon.HeaderEntry{
		Key:   key,
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
	}
}

func encryptionOf(t *testing.T) *Encryption {
	t.Helper()
	key := make([]byte, 32)
	for index := range key {
		key[index] = byte(index)
	}
	encryption, err := AES256GCM(key)
	if err != nil {
		t.Fatalf("build the encryption: %v", err)
	}
	return encryption
}

func TestGivenAnEncryptedTopicWhenPublishingThenShouldStoreNeitherPayloadNorHeaders(t *testing.T) {
	server := &sealingServer{}
	handle := FromClient(server, Options{DefaultStream: "fleet"})
	topic := handle.Topic("telemetry").WithEncryption(encryptionOf(t))

	if _, err := topic.Send(context.Background(), []byte("safe mode engaged"),
		[]iggcon.HeaderEntry{stringHeader(t, "mode", "safe")}, ""); err != nil {
		t.Fatalf("publish: %v", err)
	}

	stored := server.stored()
	if len(stored) != 1 {
		t.Fatalf("want one record, got %d", len(stored))
	}
	// The server holds ciphertext, which is the whole point: what it cannot
	// read it cannot filter, index or leak.
	if bytes.Contains(stored[0].Payload, []byte("safe mode engaged")) {
		t.Error("the payload reached the log in the clear")
	}
	if bytes.Contains(stored[0].UserHeaders, []byte("mode")) {
		t.Error("the header block reached the log in the clear")
	}
	// The header still describes the record, or a reader sizes the wrong
	// allocation off it.
	if int(stored[0].Header.PayloadLength) != len(stored[0].Payload) {
		t.Errorf("want the sealed payload length, got %d",
			stored[0].Header.PayloadLength)
	}
	if int(stored[0].Header.UserHeaderLength) != len(stored[0].UserHeaders) {
		t.Errorf("want the sealed header length, got %d",
			stored[0].Header.UserHeaderLength)
	}
}

func TestGivenASealedRecordWhenReadWithTheSameKeyThenShouldGiveBackWhatWasSent(t *testing.T) {
	encryption := encryptionOf(t)
	server := &sealingServer{}
	handle := FromClient(server, Options{DefaultStream: "fleet"})
	topic := handle.Topic("telemetry").WithEncryption(encryption)
	headers := []iggcon.HeaderEntry{stringHeader(t, "mode", "safe")}

	if _, err := topic.Send(
		context.Background(), []byte("safe mode engaged"), headers, ""); err != nil {
		t.Fatalf("publish: %v", err)
	}

	stored := server.stored()[0]
	opened, err := encryption.open(stored)
	if err != nil {
		t.Fatalf("open the record: %v", err)
	}
	if string(opened.Payload) != "safe mode engaged" {
		t.Errorf("want the payload back, got %q", opened.Payload)
	}
	if !bytes.Equal(opened.UserHeaders, iggcon.GetHeadersBytes(headers)) {
		t.Error("want the header block back exactly as it was sent")
	}
}

func TestGivenASealedRecordWhenReadWithAnotherKeyThenShouldReportItRatherThanPassCiphertext(t *testing.T) {
	server := &sealingServer{}
	handle := FromClient(server, Options{DefaultStream: "fleet"})
	topic := handle.Topic("telemetry").WithEncryption(encryptionOf(t))
	if _, err := topic.Send(
		context.Background(), []byte("safe mode engaged"), nil, ""); err != nil {
		t.Fatalf("publish: %v", err)
	}
	other, err := AES256GCM(make([]byte, 32))
	if err != nil {
		t.Fatalf("build the other key: %v", err)
	}

	_, err = other.open(server.stored()[0])

	// Handing back bytes the caller cannot read, under a key it believes it
	// holds, hides the misconfiguration until something downstream fails.
	if err == nil {
		t.Fatal("a record that will not decrypt must be reported")
	}
}

func TestGivenNoEncryptionWhenPublishingThenShouldLeaveTheRecordAsItWas(t *testing.T) {
	server := &sealingServer{}
	handle := FromClient(server, Options{DefaultStream: "fleet"})

	if _, err := handle.Topic("telemetry").Send(
		context.Background(), []byte("safe mode engaged"), nil, ""); err != nil {
		t.Fatalf("publish: %v", err)
	}

	if string(server.stored()[0].Payload) != "safe mode engaged" {
		t.Error("a topic with no key must publish the record as it stands")
	}
}

func TestGivenAConsumerHoldingTheKeyWhenARecordArrivesThenShouldOpenItBeforeTheSpineIsRead(t *testing.T) {
	encryption := encryptionOf(t)
	server := &sealingServer{}
	handle := FromClient(server, Options{DefaultStream: "fleet"})
	topic := handle.Topic("telemetry").WithEncryption(encryption)
	if _, err := topic.Send(
		context.Background(), []byte("safe mode engaged"), nil, ""); err != nil {
		t.Fatalf("publish: %v", err)
	}
	delivered := reader.ReceivedMessage{PartitionID: 1, Message: server.stored()[0]}

	record, err := openRecord(delivered, encryption)
	if err != nil {
		t.Fatalf("open the delivered record: %v", err)
	}

	// The record is opened before its spine is read, or a sealed provenance
	// would decode as an undecodable one and go to the dead-letter topic.
	if string(record.Payload()) != "safe mode engaged" {
		t.Errorf("want the payload back, got %q", record.Payload())
	}
}

func TestGivenAConsumerWithoutTheKeyWhenASealedRecordArrivesThenShouldLeaveItSealed(t *testing.T) {
	server := &sealingServer{}
	handle := FromClient(server, Options{DefaultStream: "fleet"})
	topic := handle.Topic("telemetry").WithEncryption(encryptionOf(t))
	if _, err := topic.Send(
		context.Background(), []byte("safe mode engaged"), nil, ""); err != nil {
		t.Fatalf("publish: %v", err)
	}
	delivered := reader.ReceivedMessage{PartitionID: 1, Message: server.stored()[0]}

	record, err := openRecord(delivered, nil)
	if err != nil {
		t.Fatalf("a consumer with no key reads what is stored: %v", err)
	}

	// Nothing guesses at a key. A reader without one is handed the bytes the
	// log holds, which is what a reader that is not meant to see them gets.
	if string(record.Payload()) == "safe mode engaged" {
		t.Error("a consumer with no key must not read the plaintext")
	}
}
