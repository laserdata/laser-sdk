package laser

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

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
