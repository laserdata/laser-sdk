package laser

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/iggy/foreign/go/client/tcp"
)

func TestGivenHandlesOverAClientThatHoldsItsPasswordWhenFormattedThenShouldNeverPrintIt(t *testing.T) {
	const password = "s3cr3t-upstream-pass"
	upstream := tcp.NewIggyTcpClient(nil,
		tcp.WithServerAddress("127.0.0.1:1"),
		tcp.WithAutoLogin(tcp.NewUsernamePasswordCredentials("agent", password)))
	t.Cleanup(func() { _ = upstream.Close() })
	configured := OpenCapabilities()
	handle := &Laser{
		shared:         &connection{client: upstream, configured: &configured},
		connectionInfo: func() *ConnectionString { return &ConnectionString{Host: "127.0.0.1", Port: 1} },
		capabilities:   configured,
	}
	topic := handle.Stream("fleet").Topic("records")
	subjects := map[string]any{
		"handle": handle, "handle value": *handle, "topic": topic,
		"group": topic.Group("readers"), "reader": &GroupReader{client: &heldClient{upstream}},
		"reader value": GroupReader{client: &heldClient{upstream}},
	}
	for name, subject := range subjects {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d"} {
			text := fmt.Sprintf(verb, subject)
			if strings.Contains(text, password) || strings.Contains(strings.ToLower(text), hex.EncodeToString([]byte(password))) {
				t.Errorf("%s with %s prints the password: %.240s", name, verb, text)
			}
		}
	}
}
