package transmit

import (
	"bufio"
	"compress/gzip"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

// capturingUploader records uploaded objects for inspection.
type capturingUploader struct {
	mutex   sync.Mutex
	objects map[string][]byte
}

func newCapturingUploader() *capturingUploader {
	return &capturingUploader{objects: make(map[string][]byte)}
}

func (c *capturingUploader) upload(ctx context.Context, name string, data []byte) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.objects[name] = data
	return nil
}

func (c *capturingUploader) len() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.objects)
}

func (c *capturingUploader) all() map[string][]byte {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	out := make(map[string][]byte, len(c.objects))
	for k, v := range c.objects {
		out[k] = v
	}
	return out
}

func newTestGCSTransmission(cfg config.GCSExportConfig, clock clockwork.Clock) (*GCSTransmission, *capturingUploader) {
	uploader := newCapturingUploader()
	g := &GCSTransmission{
		Config:   &config.MockConfig{GetGCSExportConfigVal: cfg},
		Logger:   &logger.NullLogger{},
		Metrics:  &metrics.NullMetrics{},
		Clock:    clock,
		uploadFn: uploader.upload,
	}
	return g, uploader
}

func testEvent(cfg config.Config, traceID string) *types.Event {
	data := types.NewPayload(cfg, map[string]any{
		"service.name":   "test-service",
		"trace.trace_id": traceID,
	})
	return &types.Event{
		Dataset:     "test-dataset",
		Environment: "test-env",
		SampleRate:  7,
		Timestamp:   time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC),
		Data:        data,
	}
}

func decodeRecords(t *testing.T, data []byte) []gcsExportRecord {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer gz.Close()

	var records []gcsExportRecord
	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		var rec gcsExportRecord
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &rec))
		records = append(records, rec)
	}
	require.NoError(t, scanner.Err())
	return records
}

func TestGCSTransmissionRequiresBucket(t *testing.T) {
	g, _ := newTestGCSTransmission(config.GCSExportConfig{Enabled: true}, clockwork.NewFakeClock())
	err := g.Start()
	assert.Error(t, err)
}

func TestGCSTransmissionFlushesOnStop(t *testing.T) {
	cfg := config.GCSExportConfig{
		Enabled:       true,
		Bucket:        "test-bucket",
		KeyPrefix:     "refinery/traces",
		FlushInterval: config.Duration(time.Minute),
		MaxBatchSize:  config.MemorySize(100 * 1024 * 1024),
		QueueSize:     100,
	}
	g, uploader := newTestGCSTransmission(cfg, clockwork.NewFakeClock())
	require.NoError(t, g.Start())

	mockCfg := &config.MockConfig{}
	for i := 0; i < 5; i++ {
		g.EnqueueSpan(&types.Span{Event: testEvent(mockCfg, "trace-1"), TraceID: "trace-1"})
	}
	require.NoError(t, g.Stop())

	require.Equal(t, 1, uploader.len())
	for name, data := range uploader.all() {
		// prefix and time-partitioned path from the event flush time
		assert.True(t, strings.HasPrefix(name, "refinery/traces/"), "object name %q should start with key prefix", name)
		assert.True(t, strings.HasSuffix(name, ".jsonl.gz"), "object name %q should end in .jsonl.gz", name)

		records := decodeRecords(t, data)
		require.Len(t, records, 5)
		for _, rec := range records {
			assert.Equal(t, "test-dataset", rec.Dataset)
			assert.Equal(t, "test-env", rec.Environment)
			assert.Equal(t, uint(7), rec.SampleRate)
			assert.Equal(t, "trace-1", rec.TraceID)
			assert.Equal(t, "test-service", rec.Data.Get("service.name"))
		}
	}
}

func TestGCSTransmissionFlushesOnBatchSize(t *testing.T) {
	cfg := config.GCSExportConfig{
		Enabled:       true,
		Bucket:        "test-bucket",
		FlushInterval: config.Duration(time.Minute),
		// tiny batch size: every event should produce its own object
		MaxBatchSize: config.MemorySize(1),
		QueueSize:    100,
	}
	g, uploader := newTestGCSTransmission(cfg, clockwork.NewFakeClock())
	require.NoError(t, g.Start())

	mockCfg := &config.MockConfig{}
	for i := 0; i < 3; i++ {
		g.EnqueueEvent(testEvent(mockCfg, "trace-2"))
	}

	assert.Eventually(t, func() bool {
		return uploader.len() == 3
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, g.Stop())

	// object names must be unique and unprefixed
	for name := range uploader.all() {
		assert.False(t, strings.HasPrefix(name, "/"), "object name %q should not start with a slash", name)
		records := decodeRecords(t, data(t, uploader, name))
		assert.Len(t, records, 1)
	}
}

// data fetches one object's bytes from the uploader.
func data(t *testing.T, u *capturingUploader, name string) []byte {
	t.Helper()
	u.mutex.Lock()
	defer u.mutex.Unlock()
	d, ok := u.objects[name]
	require.True(t, ok)
	return d
}

func TestGCSTransmissionFlushesOnInterval(t *testing.T) {
	clock := clockwork.NewFakeClock()
	cfg := config.GCSExportConfig{
		Enabled:       true,
		Bucket:        "test-bucket",
		FlushInterval: config.Duration(time.Minute),
		MaxBatchSize:  config.MemorySize(100 * 1024 * 1024),
		QueueSize:     100,
	}
	g, uploader := newTestGCSTransmission(cfg, clock)
	require.NoError(t, g.Start())

	mockCfg := &config.MockConfig{}
	g.EnqueueEvent(testEvent(mockCfg, "trace-3"))

	// wait for the event to be consumed by the flush loop before advancing
	assert.Eventually(t, func() bool {
		return len(g.events) == 0
	}, 2*time.Second, 10*time.Millisecond)

	clock.Advance(time.Minute + time.Second)

	assert.Eventually(t, func() bool {
		return uploader.len() == 1
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, g.Stop())
}

func TestGCSTransmissionDropsWhenQueueFull(t *testing.T) {
	cfg := config.GCSExportConfig{
		Enabled:       true,
		Bucket:        "test-bucket",
		FlushInterval: config.Duration(time.Minute),
		MaxBatchSize:  config.MemorySize(100 * 1024 * 1024),
		QueueSize:     1,
	}
	uploader := newCapturingUploader()
	mockMetrics := &metrics.MockMetrics{}
	mockMetrics.Start()
	g := &GCSTransmission{
		Config:   &config.MockConfig{GetGCSExportConfigVal: cfg},
		Logger:   &logger.NullLogger{},
		Metrics:  mockMetrics,
		Clock:    clockwork.NewFakeClock(),
		uploadFn: uploader.upload,
	}
	require.NoError(t, g.Start())
	// stop the flush loop so nothing drains the queue, then overfill it
	require.NoError(t, g.Stop())

	mockCfg := &config.MockConfig{}
	for i := 0; i < 10; i++ {
		g.EnqueueEvent(testEvent(mockCfg, "trace-4"))
	}

	dropped, ok := mockMetrics.Get(counterGCSExportDropped)
	require.True(t, ok)
	// queue holds 1 (plus possibly one held by the exited loop); the rest dropped
	assert.GreaterOrEqual(t, dropped, float64(8))
}

func TestGCSObjectNamePartitioning(t *testing.T) {
	g := &GCSTransmission{keyPrefix: "some/prefix", hostname: "host-1"}
	ts := time.Date(2026, 7, 7, 15, 4, 5, 0, time.UTC)
	name := g.objectName(ts)
	assert.True(t, strings.HasPrefix(name, "some/prefix/2026/07/07/15/host-1-"), "got %q", name)
	assert.True(t, strings.HasSuffix(name, "-000001.jsonl.gz"), "got %q", name)

	// sequence increments for uniqueness
	name2 := g.objectName(ts)
	assert.NotEqual(t, name, name2)
}
