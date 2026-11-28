package laser_test

import (
	"context"
	"errors"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

func TestGivenAServerWithoutTheCatalogWhenAGroupFilterVerbIsCalledThenShouldRefuseAsUnsupportedBeforeReadingTheGroup(t *testing.T) {
	ctx := context.Background()
	for name, call := range map[string]func(*laser.GroupFilter) error{
		"get":     func(filter *laser.GroupFilter) error { _, err := filter.Get(ctx); return err },
		"summary": func(filter *laser.GroupFilter) error { _, err := filter.Summary(ctx); return err },
		"revisions": func(filter *laser.GroupFilter) error {
			_, err := filter.Revisions(ctx, 1, 10)
			return err
		},
		"set revision enabled": func(filter *laser.GroupFilter) error { return filter.SetRevisionEnabled(ctx, 1, false) },
		"preview":              func(filter *laser.GroupFilter) error { _, err := filter.Preview(ctx, 0); return err },
		"test":                 func(filter *laser.GroupFilter) error { _, err := filter.Test(ctx, []byte("{}"), nil); return err },
		"release":              func(filter *laser.GroupFilter) error { _, err := filter.Release(ctx); return err },
		"delete":               func(filter *laser.GroupFilter) error { _, err := filter.Delete(ctx); return err },
	} {
		server := newGroupServer()
		filter := laser.FromClient(server, laser.Options{DefaultStream: "orders"}).
			Topic("events").Group("anomaly-desk").Filter()

		err := call(filter)

		var unsupported *laser.UnsupportedError
		if !errors.As(err, &unsupported) {
			t.Errorf("%s: want the catalog refused as unsupported for a group that does not exist, got %v", name, err)
		}
	}
}
