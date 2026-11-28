//go:build integration

package laser_test

import (
	"context"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenALiveServerWhenConnectingWithAnAddressAndCredentialsApartThenShouldLogIn(t *testing.T) {
	target, err := laser.ParseConnectionString(liveConnectionString())
	if err != nil {
		t.Fatalf("parse the live connection string: %v", err)
	}
	if target.Credentials.IsToken() {
		t.Skip("the live deployment is reached with a token, which has no username and password")
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveConnectTimeout)
	defer cancel()

	client, err := laser.ConnectWith(ctx, "", laser.Options{
		Address:  net.JoinHostPort(target.Host, strconv.Itoa(int(target.Port))),
		Username: target.Credentials.Username,
		Password: target.Credentials.Password,
		Timeout:  liveConnectTimeout,
	})
	if err != nil {
		if os.Getenv("LASER_LIVE_CONNECTION_STRING") != "" || os.Getenv("LASER_CONNECTION_STRING") != "" {
			t.Fatalf("connect to configured live deployment: %v", err)
		}
		t.Skip("no local deployment, set LASER_LIVE_CONNECTION_STRING to run the live suite")
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close the connection: %v", err)
		}
	})

	me, err := client.Me(liveContext(t))
	if err != nil {
		t.Fatalf("an authenticated call must succeed: %v", err)
	}
	if me == nil {
		t.Fatal("want the server to describe this client")
	}
	if got := client.Connection().Credentials; got != target.Credentials {
		t.Error("want the handle to carry the credentials it logged in with")
	}
}

func TestGivenALiveOpsStreamOfItsOwnWhenWatchingThroughAnOverriddenTopologyThenShouldReadItsChanges(t *testing.T) {
	client := dialLive(t)
	ctx := liveContext(t)
	scratch := liveScratch(t, client, ctx, "laser-live-ops")
	feed := scratch.Laser().Topic("feed")
	if err := feed.Ensure(ctx, 1, nil); err != nil {
		t.Fatalf("create the feed topic: %v", err)
	}
	change := wire.ChangeRecord{
		V: wire.ChangeOpVersion, Index: "readings", PartitionID: 0, FromOffset: 7, ToOffset: 7, Rows: 1,
	}
	encoded, err := wire.Marshal(change)
	if err != nil {
		t.Fatalf("encode a change: %v", err)
	}
	if _, err := feed.Publish().Payload(encoded).Send(ctx); err != nil {
		t.Fatalf("publish the change: %v", err)
	}
	capabilities := client.Capabilities()
	capabilities.Watch = true
	watching := client.WithCapabilities(capabilities).WithOpsStream(scratch.Name()).WithChangesTopic("feed")

	reader, err := watching.Watch().Records(ctx)
	if err != nil {
		t.Fatalf("open the feed on the overridden topology: %v", err)
	}
	changes, err := reader.PollWithin(ctx, 10*time.Second)
	if err != nil {
		t.Fatalf("poll the feed: %v", err)
	}

	if len(changes) != 1 || changes[0].Change != change {
		t.Fatalf("want the one change published on %s/feed, got %+v", scratch.Name(), changes)
	}
}
