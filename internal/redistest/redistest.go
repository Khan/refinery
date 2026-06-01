// Package redistest provides a shared Redis testcontainer for tests that need
// a real Redis instance. One container is started per test binary on first
// call and reused across tests; the testcontainers Reaper cleans it up when
// the process exits.
package redistest

import (
	"context"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/redis"
)

const image = "redis:6.2"

var (
	once       sync.Once
	sharedHost string
	sharedPort string
	startup    error
)

// Endpoint returns the host and port of a shared Redis container, starting it
// on first call. The container lives for the duration of the test process.
func Endpoint(t testing.TB) (host, port string) {
	t.Helper()
	once.Do(start)
	if startup != nil {
		t.Fatalf("redistest: failed to start Redis container: %v", startup)
	}
	return sharedHost, sharedPort
}

func start() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := redis.Run(ctx, image)
	if err != nil {
		startup = err
		return
	}
	conn, err := c.ConnectionString(ctx)
	if err != nil {
		startup = err
		return
	}
	u, err := url.Parse(conn)
	if err != nil {
		startup = err
		return
	}
	h, p, err := net.SplitHostPort(u.Host)
	if err != nil {
		startup = err
		return
	}
	sharedHost = h
	sharedPort = p
}
