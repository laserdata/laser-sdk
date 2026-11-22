package laser

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	aes256KeyBytes = 32
	gcmNonceBytes  = 12
	gcmTagBytes    = 16
)

// Encryption is client-side encryption in Apache Iggy's AES-256-GCM format: a
// random 12-byte nonce, the ciphertext, then the 16-byte tag. A producer
// encrypts the payload and the user-header block of each record, and a
// consumer with the same key decrypts both, so the server stores only
// ciphertext. Every Laser client and Apache Iggy's own encryptor read each
// other's records.
type Encryption struct {
	// Holding the sealed AEAD rather than the key means the key is never a
	// field anything can reach or print.
	aead cipher.AEAD
}

// AES256GCM builds an encryption from a 32-byte key. Any other key length is
// refused, because a shorter key would otherwise be silently stretched by the
// cipher and produce records no other client can read.
func AES256GCM(key []byte) (*Encryption, error) {
	if len(key) != aes256KeyBytes {
		return nil, &InvalidError{
			Reason: "an AES-256-GCM key holds " + itoa(aes256KeyBytes) +
				" bytes, not " + itoa(len(key))}
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, &InvalidError{Reason: "an AES-256-GCM key: " + err.Error()}
	}
	aead, err := cipher.NewGCMWithNonceSize(block, gcmNonceBytes)
	if err != nil {
		return nil, &InvalidError{Reason: "an AES-256-GCM key: " + err.Error()}
	}
	return &Encryption{aead: aead}, nil
}

// Encrypt seals plaintext under a fresh nonce.
//
// Reusing a nonce under one key breaks GCM outright, so the nonce comes from
// the system source rather than a counter this client would have to persist.
func (encryption *Encryption) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, gcmNonceBytes)
	if _, err := rand.Read(nonce); err != nil {
		return nil, &TransportError{Reason: "draw an encryption nonce", Cause: err}
	}
	// Seal appends the tag to the ciphertext, which is the layout the other
	// clients write, so the nonce prefix is all this has to add.
	return encryption.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt opens what Encrypt sealed. A ciphertext that is short, altered, or
// sealed with another key reports Apache Iggy's error 72, which is what the
// other clients raise, so a caller matches one error across every client.
func (encryption *Encryption) Decrypt(ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < gcmNonceBytes+gcmTagBytes {
		return nil, cannotDecrypt()
	}
	plaintext, err := encryption.aead.Open(
		nil, ciphertext[:gcmNonceBytes], ciphertext[gcmNonceBytes:], nil)
	if err != nil {
		// The underlying failure is deliberately dropped: it distinguishes a
		// bad tag from a bad key, which is a probe an attacker can run.
		return nil, cannotDecrypt()
	}
	return plaintext, nil
}

// String names the cipher without the key, so logging a client's options or a
// handle holding one cannot leak it.
func (encryption *Encryption) String() string { return "Encryption(Aes256Gcm)" }

func cannotDecrypt() error {
	return &TransportError{
		Reason: "decrypt a record", Cause: ierror.CannotDecryptData{}}
}

// seal is each record with its payload and user-header block encrypted,
// keeping the id and origin timestamp it was built with.
//
// The block goes over as raw bytes rather than as header entries, because
// ciphertext is not a header block any reader could decode. The lengths in
// the message header are restated for the sealed sizes, which is what the
// encoder writes and the server stores.
func (encryption *Encryption) seal(
	messages []iggcon.IggyMessage,
) ([]iggcon.IggyMessage, error) {
	sealed := make([]iggcon.IggyMessage, 0, len(messages))
	for _, message := range messages {
		payload, err := encryption.Encrypt(message.Payload)
		if err != nil {
			return nil, &InvalidError{Reason: "encrypt the record: " + err.Error()}
		}
		closed := iggcon.IggyMessage{Header: message.Header, Payload: payload}
		if len(message.UserHeaders) > 0 {
			block, err := encryption.Encrypt(message.UserHeaders)
			if err != nil {
				return nil, &InvalidError{
					Reason: "encrypt the record headers: " + err.Error()}
			}
			closed.UserHeaders = block
		}
		closed.Header.PayloadLength = uint32(len(closed.Payload))
		closed.Header.UserHeaderLength = uint32(len(closed.UserHeaders))
		sealed = append(sealed, closed)
	}
	return sealed, nil
}

// open is one record with its payload and user-header block decrypted.
//
// A record that will not decrypt is reported rather than passed on as
// ciphertext: handing a caller bytes it cannot read, under a key it believes
// it holds, hides the misconfiguration until something downstream fails.
func (encryption *Encryption) open(
	message iggcon.IggyMessage,
) (iggcon.IggyMessage, error) {
	payload, err := encryption.Decrypt(message.Payload)
	if err != nil {
		return iggcon.IggyMessage{}, &CodecError{
			Reason: "decrypt the record", Cause: err}
	}
	opened := iggcon.IggyMessage{Header: message.Header, Payload: payload}
	if len(message.UserHeaders) > 0 {
		block, err := encryption.Decrypt(message.UserHeaders)
		if err != nil {
			return iggcon.IggyMessage{}, &CodecError{
				Reason: "decrypt the record headers", Cause: err}
		}
		opened.UserHeaders = block
	}
	opened.Header.PayloadLength = uint32(len(opened.Payload))
	opened.Header.UserHeaderLength = uint32(len(opened.UserHeaders))
	return opened, nil
}
