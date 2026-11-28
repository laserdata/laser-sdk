//go:build integration

package laser_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenALiveHandleWhenFormattedOrRefusedThenShouldNeverShowItsPassword(t *testing.T) {
	root := dialLive(t)
	ctx := liveContext(t)
	const password, wrong = "s3cr3t:p@ss-live", "wr0ng:p@ss-live"
	name := liveName("redaction-user")
	if _, err := root.User(name).Create(ctx, password, laser.CreateUserOptions{Status: laser.UserActive}); err != nil {
		t.Fatalf("create the user: %v", err)
	}
	t.Cleanup(func() { _, _ = root.User(name).Delete(context.WithoutCancel(ctx)) })
	address := fmt.Sprintf("%s:%d", root.Connection().Host, root.Connection().Port)

	client, err := laser.ConnectWith(ctx, "", laser.Options{
		Address: address, Username: name, Password: password, DefaultStream: "redaction", Timeout: liveConnectTimeout})
	if err != nil {
		t.Fatalf("connect as the user: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	topic := client.Topic("records")
	subjects := map[string]any{
		"handle": client, "handle value": *client, "connection": client.Connection(),
		"topic": topic, "kv": client.Kv("notes"), "sessions": client.Sessions(),
		"memory": client.Memory("facts"), "group": topic.Group("readers"),
	}
	secrets := []string{password, "s3cr3t%3Ap%40ss-live", hex.EncodeToString([]byte(password))}
	for label, subject := range subjects {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x"} {
			text := fmt.Sprintf(verb, subject)
			for _, secret := range secrets {
				if strings.Contains(text, secret) {
					t.Errorf("%s with %s shows the password: %.200s", label, verb, text)
				}
			}
		}
	}
	encoded, err := json.Marshal(client.Connection())
	if err != nil || strings.Contains(string(encoded), password) {
		t.Errorf("the marshalled connection shows the password: %s, %v", encoded, err)
	}

	_, err = laser.ConnectWith(ctx, "", laser.Options{
		Address: address, Username: name, Password: wrong, Timeout: liveConnectTimeout})
	if err == nil {
		t.Fatal("want a wrong password refused")
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		text := fmt.Sprintf(verb, err)
		for _, secret := range []string{wrong, "wr0ng%3Ap%40ss-live", password} {
			if strings.Contains(text, secret) {
				t.Errorf("a refused sign-in with %s shows a password: %s", verb, text)
			}
		}
	}
}
