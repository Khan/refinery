package transmit

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/jonboulle/clockwork"
	"github.com/sourcegraph/conc/pool"

	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/types"
)

const (
	counterGCSExportEvents       = "gcs_export_events"
	counterGCSExportDropped      = "gcs_export_dropped"
	counterGCSExportBatches      = "gcs_export_batches"
	counterGCSExportErrors       = "gcs_export_errors"
	histogramGCSExportBatchBytes = "gcs_export_batch_bytes"
	gaugeGCSExportQueueLength    = "gcs_export_queue_length"

	// number of batch uploads that may be in flight at once; when they're all
	// busy the flusher blocks, the queue fills, and new spans are dropped from
	// the export rather than blocking the collector.
	maxConcurrentGCSUploads = 4

	// how long a single object upload may take before it is abandoned
	gcsUploadTimeout = 60 * time.Second
)

var gcsTransmissionMetrics = []metrics.Metadata{
	{Name: counterGCSExportEvents, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of events written to the GCS export"},
	{Name: counterGCSExportDropped, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of events dropped from the GCS export because the queue was full"},
	{Name: counterGCSExportBatches, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of batch objects written to GCS"},
	{Name: counterGCSExportErrors, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of errors encountered while writing batches to GCS"},
	{Name: histogramGCSExportBatchBytes, Type: metrics.Histogram, Unit: metrics.Bytes, Description: "compressed size of batch objects written to GCS"},
	{Name: gaugeGCSExportQueueLength, Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of events waiting to be batched for the GCS export"},
}

// gcsExportRecord is the shape of one line in an exported JSON Lines object.
type gcsExportRecord struct {
	Time        time.Time     `json:"time"`
	TraceID     string        `json:"trace_id,omitempty"`
	Dataset     string        `json:"dataset"`
	Environment string        `json:"environment,omitempty"`
	SampleRate  uint          `json:"sample_rate"`
	Data        types.Payload `json:"data"`
}

// gcsUploader writes a finished (compressed) batch to an object. It exists so
// tests can capture uploads without a real GCS client.
type gcsUploader func(ctx context.Context, objectName string, data []byte) error

// gcsExportItem carries an event through the export queue along with its
// trace ID, which lives on the Span rather than the Event.
type gcsExportItem struct {
	ev      *types.Event
	traceID string
}

// GCSTransmission implements Transmission by batching events into gzipped
// JSON Lines objects and writing them to a GCS bucket under time-partitioned
// object names. Enqueueing never blocks: if the internal queue is full
// (because GCS is slow or unavailable), events are dropped from the export
// and counted in gcs_export_dropped.
type GCSTransmission struct {
	Config  config.Config   `inject:""`
	Logger  logger.Logger   `inject:""`
	Metrics metrics.Metrics `inject:"metrics"`
	Clock   clockwork.Clock `inject:""`

	// uploadFn may be set before Start to override how batches are written
	// (used in tests). If nil, a real GCS client is created at Start.
	uploadFn gcsUploader

	client     *storage.Client
	bucket     string
	keyPrefix  string
	flushEvery time.Duration
	maxBatch   int

	hostname   string
	seq        uint64
	events     chan gcsExportItem
	done       chan struct{}
	flushWG    sync.WaitGroup
	uploadPool *pool.Pool
}

func NewGCSTransmission() *GCSTransmission {
	return &GCSTransmission{}
}

func (g *GCSTransmission) Start() error {
	cfg := g.Config.GetGCSExportConfig()
	if cfg.Bucket == "" {
		return fmt.Errorf("GCSExport is enabled but no Bucket is configured")
	}
	g.bucket = cfg.Bucket
	g.keyPrefix = strings.Trim(cfg.KeyPrefix, "/")
	g.flushEvery = time.Duration(cfg.FlushInterval)
	if g.flushEvery <= 0 {
		g.flushEvery = time.Minute
	}
	g.maxBatch = int(cfg.MaxBatchSize)

	if g.Clock == nil {
		g.Clock = clockwork.NewRealClock()
	}

	for _, m := range gcsTransmissionMetrics {
		g.Metrics.Register(m)
	}

	g.hostname, _ = os.Hostname()
	if g.hostname == "" {
		g.hostname = "unknown"
	}

	if g.uploadFn == nil {
		client, err := storage.NewClient(context.Background())
		if err != nil {
			return fmt.Errorf("failed to create GCS client for trace export: %w", err)
		}
		g.client = client
		g.uploadFn = g.uploadObject
	}

	g.events = make(chan gcsExportItem, cfg.QueueSize)
	g.done = make(chan struct{})
	g.uploadPool = pool.New().WithMaxGoroutines(maxConcurrentGCSUploads)
	g.flushWG.Add(1)
	go g.flushLoop()

	g.Logger.Info().WithString("bucket", g.bucket).Logf("started GCS trace export")
	return nil
}

func (g *GCSTransmission) Stop() error {
	close(g.done)
	g.flushWG.Wait()
	g.uploadPool.Wait()
	if g.client != nil {
		g.client.Close()
	}
	return nil
}

// EnqueueEvent queues an event for export to GCS. It never blocks; when the
// queue is full the event is dropped from the export.
func (g *GCSTransmission) EnqueueEvent(ev *types.Event) {
	g.enqueue(gcsExportItem{ev: ev, traceID: ev.Data.MetaTraceID})
}

func (g *GCSTransmission) EnqueueSpan(sp *types.Span) {
	g.enqueue(gcsExportItem{ev: sp.Event, traceID: sp.TraceID})
}

func (g *GCSTransmission) enqueue(item gcsExportItem) {
	select {
	case g.events <- item:
	default:
		g.Metrics.Increment(counterGCSExportDropped)
	}
}

// flushLoop drains the event queue, serializing events into a gzipped JSON
// Lines buffer, and hands the buffer off for upload when it exceeds the
// configured batch size or the flush interval elapses.
func (g *GCSTransmission) flushLoop() {
	defer g.flushWG.Done()

	ticker := g.Clock.NewTicker(g.flushEvery)
	defer ticker.Stop()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	var uncompressed, count int

	flush := func() {
		if count == 0 {
			return
		}
		if err := gz.Close(); err != nil {
			g.Metrics.Increment(counterGCSExportErrors)
			g.Logger.Error().WithString("error", err.Error()).Logf("failed to finish compressing GCS export batch")
		} else {
			g.dispatchUpload(buf.Bytes(), count)
		}
		buf = bytes.Buffer{}
		gz = gzip.NewWriter(&buf)
		uncompressed, count = 0, 0
	}

	add := func(item gcsExportItem) {
		ev := item.ev
		g.Metrics.Gauge(gaugeGCSExportQueueLength, float64(len(g.events)))
		line, err := json.Marshal(gcsExportRecord{
			Time:        ev.Timestamp,
			TraceID:     item.traceID,
			Dataset:     ev.Dataset,
			Environment: ev.Environment,
			SampleRate:  ev.SampleRate,
			Data:        ev.Data,
		})
		if err != nil {
			g.Metrics.Increment(counterGCSExportErrors)
			g.Logger.Error().WithString("error", err.Error()).Logf("failed to serialize event for GCS export")
			return
		}
		gz.Write(line)
		gz.Write([]byte{'\n'})
		uncompressed += len(line) + 1
		count++
		g.Metrics.Increment(counterGCSExportEvents)
		if uncompressed >= g.maxBatch {
			flush()
		}
	}

	for {
		select {
		case item := <-g.events:
			add(item)
		case <-ticker.Chan():
			flush()
		case <-g.done:
			// drain whatever is still queued, then do a final flush
			for {
				select {
				case item := <-g.events:
					add(item)
				default:
					flush()
					return
				}
			}
		}
	}
}

// dispatchUpload hands a finished batch to the upload pool. If all upload
// slots are busy this blocks, which in turn causes the queue to fill and
// events to be dropped from the export — never blocking the collector.
func (g *GCSTransmission) dispatchUpload(data []byte, count int) {
	name := g.objectName(g.Clock.Now().UTC())
	g.uploadPool.Go(func() {
		ctx, cancel := context.WithTimeout(context.Background(), gcsUploadTimeout)
		defer cancel()
		if err := g.uploadFn(ctx, name, data); err != nil {
			g.Metrics.Increment(counterGCSExportErrors)
			g.Logger.Error().
				WithString("error", err.Error()).
				WithString("object", name).
				WithField("num_events", count).
				Logf("failed to write trace export batch to GCS")
			return
		}
		g.Metrics.Increment(counterGCSExportBatches)
		g.Metrics.Histogram(histogramGCSExportBatchBytes, float64(len(data)))
	})
}

// objectName returns a time-partitioned object name like
// prefix/2026/07/07/15/hostname-1783100000-000001.jsonl.gz
func (g *GCSTransmission) objectName(t time.Time) string {
	g.seq++
	name := fmt.Sprintf("%04d/%02d/%02d/%02d/%s-%d-%06d.jsonl.gz",
		t.Year(), t.Month(), t.Day(), t.Hour(), g.hostname, t.Unix(), g.seq)
	if g.keyPrefix != "" {
		name = g.keyPrefix + "/" + name
	}
	return name
}

// uploadObject writes one batch to GCS. Object names are unique, so we use a
// does-not-exist precondition, which also makes retries idempotent.
func (g *GCSTransmission) uploadObject(ctx context.Context, objectName string, data []byte) error {
	w := g.client.Bucket(g.bucket).
		Object(objectName).
		If(storage.Conditions{DoesNotExist: true}).
		NewWriter(ctx)
	w.ContentType = "application/gzip"
	if _, err := w.Write(data); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}

// NoopTransmission implements Transmission and discards everything it
// receives. It is injected in place of optional transmissions (like the GCS
// export) when they are disabled.
type NoopTransmission struct{}

func (n *NoopTransmission) EnqueueEvent(ev *types.Event) {}
func (n *NoopTransmission) EnqueueSpan(sp *types.Span)   {}
