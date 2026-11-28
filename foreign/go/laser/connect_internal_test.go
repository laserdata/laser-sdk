package laser

import (
	"errors"
	"testing"
)

func TestGivenAPasswordWithAColonAndAnAtSignWhenGivenAsCredentialsThenShouldLogInWithItExactly(t *testing.T) {
	options := Options{Address: "127.0.0.1", Username: "agent:one", Password: "pa:ss@w?rd%/#"}

	resolved, err := options.connectionString("")
	if err != nil {
		t.Fatalf("any credential characters are accepted: %v", err)
	}
	parsed, err := ParseConnectionStringWithEnv(resolved, EnvMap(nil))
	if err != nil {
		t.Fatalf("the resolved string must parse: %v", err)
	}

	want := ConnectionString{
		Host: "127.0.0.1", Port: 8090,
		Credentials: Credentials{Username: "agent:one", Password: "pa:ss@w?rd%/#"},
	}
	got := ConnectionString{Host: parsed.Host, Port: parsed.Port, Credentials: parsed.Credentials}
	if got != want {
		t.Errorf("want %+v, got %+v", want, got)
	}
}

func TestGivenAnAddressWhenResolvedThenShouldKeepItsHostAndDefaultItsPort(t *testing.T) {
	for address, want := range map[string]ConnectionString{
		"fleet.example":      {Host: "fleet.example", Port: 8090},
		"fleet.example:9100": {Host: "fleet.example", Port: 9100},
		"[::1]":              {Host: "::1", Port: 8090},
		"[::1]:9100":         {Host: "::1", Port: 9100},
	} {
		t.Run(address, func(t *testing.T) {
			resolved, err := Options{Address: address, Username: "iggy", Password: "secret"}.connectionString("")
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			parsed, err := ParseConnectionStringWithEnv(resolved, EnvMap(nil))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			got := ConnectionString{Host: parsed.Host, Port: parsed.Port}
			if got != want {
				t.Errorf("want %+v, got %+v", want, got)
			}
		})
	}
}

func TestGivenAHalfGivenConnectionWhenResolvedThenShouldRefuseAsConfig(t *testing.T) {
	for name, scenario := range map[string]struct {
		given   string
		options Options
		want    string
	}{
		"nothing given": {
			want: "a connection string, or an address with credentials, is required"},
		"address without credentials": {
			options: Options{Address: "127.0.0.1:8090"},
			want:    "credentials are required"},
		"address with a password only": {
			options: Options{Address: "127.0.0.1:8090", Password: "secret"},
			want:    "credentials are required"},
		"credentials without an address": {
			options: Options{Username: "iggy", Password: "secret"},
			want:    "address is required"},
		"an address beside a connection string": {
			given:   "iggy:secret@127.0.0.1:8090",
			options: Options{Address: "127.0.0.1:8090", Username: "iggy", Password: "secret"},
			want:    "an address or credentials conflict with a connection string already given"},
		"credentials beside a connection string": {
			given:   "iggy:secret@127.0.0.1:8090",
			options: Options{Username: "iggy"},
			want:    "an address or credentials conflict with a connection string already given"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := scenario.options.connectionString(scenario.given)
			var config *ConfigError
			if !errors.As(err, &config) {
				t.Fatalf("want a ConfigError, got %T %v", err, err)
			}
			if config.Reason != scenario.want {
				t.Errorf("want %q, got %q", scenario.want, config.Reason)
			}
		})
	}
}

func TestGivenOnlyAConnectionStringWhenResolvedThenShouldCarryItUnchanged(t *testing.T) {
	resolved, err := Options{}.connectionString("iggy://iggy:secret@fleet.example")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved != "iggy://iggy:secret@fleet.example" {
		t.Errorf("want the string carried, got %q", resolved)
	}
}
