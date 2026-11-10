// Package infratest finds the services that integration tests run against
// (deploy/compose.yaml).
package infratest

import (
	"os"
	"testing"
)

// ValkeyAddr returns the Valkey address named by RECSERVE_VALKEY_ADDR.
// Without it the test is skipped, or fails if RECSERVE_REQUIRE_INFRA is 1,
// as in CI.
func ValkeyAddr(t testing.TB) string {
	t.Helper()
	return env(t, "RECSERVE_VALKEY_ADDR")
}

func env(t testing.TB, name string) string {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		if os.Getenv("RECSERVE_REQUIRE_INFRA") == "1" {
			t.Fatalf("RECSERVE_REQUIRE_INFRA is 1 but %s is not set", name)
		}
		t.Skipf("%s not set", name)
	}
	return v
}
