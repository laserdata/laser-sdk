package laser_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenCredentialsWhenFormattedWithAnyVerbThenShouldNeverPrintTheSecret(t *testing.T) {
	const password, token = "s3cr3t-pass", "tok-abc123"
	withPassword := laser.Credentials{Username: "agent", Password: password}
	withToken := laser.Credentials{Token: token}
	connection := laser.ConnectionString{Host: "fleet.example", Port: 8090, Credentials: withPassword}
	tokenConnection := laser.ConnectionString{Host: "fleet.example", Port: 8090, Credentials: withToken}
	options := laser.Options{Address: "fleet.example:8090", Username: "agent", Password: password, DefaultStream: "fleet"}
	subjects := map[string]struct {
		value   any
		present string
	}{
		"credentials":             {withPassword, "agent"},
		"credentials pointer":     {&withPassword, "agent"},
		"token credentials":       {withToken, ""},
		"token pointer":           {&withToken, ""},
		"connection":              {connection, "fleet.example"},
		"connection pointer":      {&connection, "fleet.example"},
		"token connection":        {tokenConnection, "fleet.example"},
		"token connection ptr":    {&tokenConnection, "fleet.example"},
		"options":                 {options, "fleet.example:8090"},
		"options pointer":         {&options, "fleet.example:8090"},
		"credentials in a struct": {struct{ C laser.Credentials }{withPassword}, "agent"},
	}
	for name, subject := range subjects {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d"} {
			text := fmt.Sprintf(verb, subject.value)
			for _, secret := range []string{password, token} {
				encoded := hex.EncodeToString([]byte(secret))
				if strings.Contains(text, secret) || strings.Contains(strings.ToLower(text), encoded) {
					t.Errorf("%s with %s prints the secret: %s", name, verb, text)
				}
			}
			if verb == "%+v" && subject.present != "" && !strings.Contains(text, subject.present) {
				t.Errorf("%s with %s lost %q: %s", name, verb, subject.present, text)
			}
		}
	}
	if text := withPassword.String(); !strings.Contains(text, "agent") || strings.Contains(text, password) {
		t.Errorf("String must name the user and hide the password, got %s", text)
	}
	if text := withToken.GoString(); strings.Contains(text, token) {
		t.Errorf("GoString must hide the token, got %s", text)
	}
}

func TestGivenACoordinationClientWhenFormattedWithAnyVerbThenShouldNeverPrintItsConnectionString(t *testing.T) {
	const password = "s3cr3t-pass"
	transport := laser.NewDedicatedKvTransport("agent:" + password + "@fleet.example:8090")
	client := laser.NewDedicatedFencedLeaseClient("agent:" + password + "@fleet.example:8090")
	t.Cleanup(func() { _ = client.Close() })
	for name, subject := range map[string]any{"transport": transport, "client": client} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x"} {
			text := fmt.Sprintf(verb, subject)
			if strings.Contains(text, password) || strings.Contains(text, hex.EncodeToString([]byte(password))) {
				t.Errorf("%s with %s prints the password: %s", name, verb, text)
			}
		}
	}
}

func TestGivenAConnectThatFailsWhenItsErrorIsPrintedThenShouldNeverHoldTheSecret(t *testing.T) {
	const password, token = "s3cr3t:p@ss", "tok-abc123"
	failures := map[string]func() error{
		"credentials apart, nothing listening": func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := laser.ConnectWith(ctx, "", laser.Options{Address: "127.0.0.1:1", Username: "agent", Password: password})
			return err
		},
		"credentials apart, malformed address": func() error {
			_, err := laser.ConnectWith(context.Background(), "", laser.Options{Address: "fleet.example:notaport", Username: "agent", Password: password})
			return err
		},
		"credentials beside a connection string": func() error {
			_, err := laser.ConnectWith(context.Background(), "agent:"+password+"@127.0.0.1:1", laser.Options{Address: "127.0.0.1:1", Username: "agent", Password: password})
			return err
		},
		"token, nothing listening": func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, err := laser.Connect(ctx, token+"@127.0.0.1:1")
			return err
		},
		"token, malformed option": func() error {
			_, err := laser.Connect(context.Background(), token+"@127.0.0.1:8090?tls=maybe")
			return err
		},
	}
	for name, fail := range failures {
		err := fail()
		if err == nil {
			t.Errorf("%s: want a failure", name)
			continue
		}
		for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
			text := fmt.Sprintf(verb, err)
			for _, secret := range []string{password, token, "s3cr3t%3Ap%40ss"} {
				if strings.Contains(text, secret) {
					t.Errorf("%s with %s prints the secret: %s", name, verb, text)
				}
			}
		}
	}
}

func TestGivenASigningKeyWhenFormattedWithAnyVerbThenShouldNeverPrintThePrivateKey(t *testing.T) {
	seed := [laser.SigningSeedBytes]byte{}
	for index := range seed {
		seed[index] = byte(0xA0 + index%16)
	}
	key, err := laser.SigningKeyFromSeed(seed[:])
	if err != nil {
		t.Fatalf("build the key: %v", err)
	}
	secretHex := hex.EncodeToString(seed[:])
	decimal := strings.Trim(fmt.Sprint(seed[:8]), "[]")
	subjects := map[string]any{
		"pointer": key, "value": *key, "in a struct": struct{ K *laser.SigningKey }{key},
		"behind an unexported field":          struct{ k *laser.SigningKey }{key},
		"by value behind an unexported field": struct{ k laser.SigningKey }{*key},
		"behind a pointer to a struct":        &struct{ k *laser.SigningKey }{key},
	}
	for name, subject := range subjects {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d"} {
			text := fmt.Sprintf(verb, subject)
			if strings.Contains(strings.ToLower(text), secretHex) || strings.Contains(text, decimal) {
				t.Errorf("%s with %s prints the private key: %s", name, verb, text)
			}
		}
	}
}

func TestGivenCredentialsWhenMarshalledOrLoggedThenShouldNeverWriteTheSecret(t *testing.T) {
	const password, token = "s3cr3t-pass", "tok-abc123"
	withPassword := laser.Credentials{Username: "agent", Password: password}
	withToken := laser.Credentials{Token: token}
	connection := laser.ConnectionString{Host: "fleet.example", Port: 8090, Credentials: withPassword}
	settings := laser.TCPSettings{Address: "fleet.example:8090", Credentials: withToken}
	subjects := map[string]any{
		"credentials": withPassword, "credentials pointer": &withPassword,
		"token credentials": withToken, "connection": connection, "connection pointer": &connection,
		"settings": settings, "settings pointer": &settings,
		"credentials in a map": map[string]any{"credentials": withPassword},
	}
	for name, subject := range subjects {
		encoded, err := json.Marshal(subject)
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		var logged bytes.Buffer
		slog.New(slog.NewJSONHandler(&logged, nil)).Info("connected", "subject", subject)
		slog.New(slog.NewTextHandler(&logged, nil)).Info("connected", "subject", subject)
		for _, text := range []string{string(encoded), logged.String()} {
			if strings.Contains(text, password) || strings.Contains(text, token) {
				t.Errorf("%s writes the secret: %s", name, text)
			}
		}
	}
	encoded, err := json.Marshal(withPassword)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if want := `{"Username":"agent","Password":"[redacted]","Token":""}`; string(encoded) != want {
		t.Errorf("want %s, got %s", want, encoded)
	}
}

func TestGivenAnEncryptionKeyWhenItsCipherIsFormattedWithAnyVerbThenShouldNeverPrintTheKey(t *testing.T) {
	key := make([]byte, 32)
	for index := range key {
		key[index] = byte(0xA0 + index%16)
	}
	encryption, err := laser.AES256GCM(key)
	if err != nil {
		t.Fatalf("build the cipher: %v", err)
	}
	// The first round key word is the first four key bytes, little endian, as hex and as a decimal.
	const wordHex, wordDecimal = "a3a2a1a0", "2745344416"
	subjects := map[string]any{
		"pointer": encryption, "value": *encryption,
		"behind an unexported field":          struct{ e *laser.Encryption }{encryption},
		"by value behind an unexported field": struct{ e laser.Encryption }{*encryption},
		"in a struct":                         struct{ E *laser.Encryption }{encryption},
	}
	for name, subject := range subjects {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d"} {
			text := strings.ToLower(fmt.Sprintf(verb, subject))
			if strings.Contains(text, wordHex) || strings.Contains(text, wordDecimal) || strings.Contains(text, "160 161 162 163") {
				t.Errorf("%s with %s prints key material: %.200s", name, verb, text)
			}
		}
	}
}
