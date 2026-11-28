package laser_test

import (
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenOpenCapabilitiesWhenAskedThenShouldBeOpenOnlyWhateverTheProbeAnswered(t *testing.T) {
	for _, outcome := range []laser.HelloOutcome{
		laser.HelloUnknown, laser.HelloAnswered, laser.HelloRejected, laser.HelloFailed,
	} {
		caps := laser.OpenCapabilities()
		caps.Hello = outcome
		caps.Backends = []wire.BackendDescriptor{}
		if !caps.IsOpenOnly() {
			t.Fatalf("open capabilities with the probe %s must be open only", outcome)
		}
	}
}

func TestGivenAnyAdvertisedSurfaceWhenAskedThenShouldNotBeOpenOnly(t *testing.T) {
	for name, advertise := range map[string]func(*laser.Capabilities){
		"managed":           func(caps *laser.Capabilities) { caps.Managed = true },
		"query":             func(caps *laser.Capabilities) { caps.Query.Available = true },
		"query consistency": func(caps *laser.Capabilities) { caps.Query.Consistency = wire.ConsistencyReadYourWrites },
		"query keyword":     func(caps *laser.Capabilities) { caps.Query.Keyword = true },
		"kv":                func(caps *laser.Capabilities) { caps.Kv.Available = true },
		"kv cas":            func(caps *laser.Capabilities) { caps.Kv.CAS = true },
		"kv cas fenced":     func(caps *laser.Capabilities) { caps.Kv.CASFenced = true },
		"kv fenced leases":  func(caps *laser.Capabilities) { caps.Kv.FencedLeases = true },
		"graph":             func(caps *laser.Capabilities) { caps.Graph = true },
		"forks":             func(caps *laser.Capabilities) { caps.Forks = true },
		"a2a gateway":       func(caps *laser.Capabilities) { caps.A2AGateway = true },
		"sessions":          func(caps *laser.Capabilities) { caps.Sessions = true },
		"materializations":  func(caps *laser.Capabilities) { caps.Materializations = true },
		"stream tenancy":    func(caps *laser.Capabilities) { caps.StreamTenancy = true },
		"watch":             func(caps *laser.Capabilities) { caps.Watch = true },
		"authz":             func(caps *laser.Capabilities) { caps.Authz = true },
		"native filters":    func(caps *laser.Capabilities) { caps.Filters.Native = true },
		"filter catalog":    func(caps *laser.Capabilities) { caps.Filters.Catalog = true },
		"group policy":      func(caps *laser.Capabilities) { caps.Filters.GroupPolicyReads = true },
		"filter evaluation": func(caps *laser.Capabilities) { caps.Filters.Evaluation = &wire.FilterAnnounce{} },
		"filter wait":       func(caps *laser.Capabilities) { caps.Filters.MaxWait = time.Second },
		"versions":          func(caps *laser.Capabilities) { caps.Versions = &wire.OpVersions{} },
		"backends":          func(caps *laser.Capabilities) { caps.Backends = []wire.BackendDescriptor{{}} },
	} {
		caps := laser.OpenCapabilities()
		advertise(&caps)
		if caps.IsOpenOnly() {
			t.Fatalf("capabilities advertising %s must not be open only", name)
		}
	}
}
