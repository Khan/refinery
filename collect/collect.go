package collect

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	rtmetrics "runtime/metrics"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/dgryski/go-wyhash"
	"github.com/jonboulle/clockwork"
	"github.com/sirupsen/logrus"

	"github.com/honeycombio/refinery/collect/cache"
	"github.com/honeycombio/refinery/config"
	"github.com/honeycombio/refinery/internal/health"
	"github.com/honeycombio/refinery/internal/otelutil"
	"github.com/honeycombio/refinery/internal/peer"
	"github.com/honeycombio/refinery/logger"
	"github.com/honeycombio/refinery/metrics"
	"github.com/honeycombio/refinery/pubsub"
	"github.com/honeycombio/refinery/sample"
	"github.com/honeycombio/refinery/sharder"
	"github.com/honeycombio/refinery/transmit"
	"github.com/honeycombio/refinery/types"
)

const (
	keptTraceDecisionTopic            = "trace_decision_kept"
	dropTraceDecisionTopic            = "trace_decision_dropped"
	decisionMessageBufferSize         = 10_000
	defaultDropDecisionTickerInterval = 1 * time.Second
	defaultKeptDecisionTickerInterval = 1 * time.Second

	collectorHealthKey = "collector"
)

var ErrWouldBlock = errors.New("Dropping span as channel buffer is full. Span will not be processed and will be lost.")

type Collector interface {
	// AddSpan adds a span to be collected, buffered, and merged into a trace.
	// Once the trace is "complete", it'll be passed off to the sampler then
	// scheduled for transmission.
	AddSpan(*types.Span) error
	AddSpanFromPeer(*types.Span) error
	Stressed() bool
	GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string)
	ProcessSpanImmediately(sp *types.Span) (processed bool, keep bool)
}

func GetCollectorImplementation(c config.Config) Collector {
	return &InMemCollector{}
}

// These are the names of the metrics we use to track our send decisions.
const (
	TraceSendGotRoot        = "trace_send_got_root"
	TraceSendExpired        = "trace_send_expired"
	TraceSendSpanLimit      = "trace_send_span_limit"
	TraceSendEjectedFull    = "trace_send_ejected_full"
	TraceSendEjectedMemsize = "trace_send_ejected_memsize"
	TraceSendLateSpan       = "trace_send_late_span"
)

type sendableTrace struct {
	*types.Trace
	reason          string
	sendReason      string
	sampleKey       string
	shouldSend      bool
	rate            uint
	samplerSelector string
}

// InMemCollector is a collector that can use multiple concurrent workers to make sampling decision.
type InMemCollector struct {
	Config  config.Config   `inject:""`
	Logger  logger.Logger   `inject:""`
	Clock   clockwork.Clock `inject:""`
	Tracer  trace.Tracer    `inject:"tracer"`
	Health  health.Recorder `inject:""`
	Sharder sharder.Sharder `inject:""`

	Transmission     transmit.Transmission  `inject:"upstreamTransmission"`
	PeerTransmission transmit.Transmission  `inject:"peerTransmission"`
	PubSub           pubsub.PubSub          `inject:""`
	Metrics          metrics.Metrics        `inject:"metrics"`
	SamplerFactory   *sample.SamplerFactory `inject:""`
	StressRelief     StressReliever         `inject:"stressRelief"`
	Peers            peer.Peers             `inject:""`

	// For test use only
	TestMode       bool
	BlockOnAddSpan bool

	// Workers for parallel collection - each worker processes a subset of traces
	workers []*CollectorWorker

	// mutex must be held whenever non-channel internal fields are accessed.
	mutex sync.RWMutex

	monitorWG    sync.WaitGroup // WaitGroup for background monitoring goroutine
	workersWG    sync.WaitGroup // Separate WaitGroup for workers
	sendTracesWG sync.WaitGroup
	reload       chan struct{}      // Channel for config reload signals
	tracesToSend chan sendableTrace // Channel of traces ready for transmission
	done         chan struct{}

	hostname string

	memMetricSample    []rtmetrics.Sample // Memory monitoring using runtime/metrics
	spanCounters []config.SpanCounter
}

// These are the names of the metrics we use to track the number of events sent to peers through the router.
// Defining them here to avoid computing the names in the hot path of the collector.
const (
	peerRouterPeerMetricName     = "peer_router_peer"
	incomingRouterPeerMetricName = "incoming_router_peer"
)

var inMemCollectorMetrics = []metrics.Metadata{
	{Name: "trace_duration_ms", Type: metrics.Histogram, Unit: metrics.Milliseconds, Description: "time taken to process a trace from arrival to send"},
	{Name: "trace_span_count", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "number of spans in a trace"},
	{Name: "collector_incoming_queue", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "number of spans currently in the incoming queue"},
	{Name: "collector_peer_queue_length", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of spans in the peer queue"},
	{Name: "collector_peer_queue_capacity", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "configured maximum number of spans in the peer queue"},
	{Name: "collector_incoming_queue_length", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of spans in the incoming queue"},
	{Name: "collector_incoming_queue_capacity", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "configured maximum number of spans in the incoming queue"},
	{Name: "collector_peer_queue", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "number of spans currently in the peer queue"},
	{Name: "collector_cache_size", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of traces currently stored in the trace cache"},
	{Name: "collect_cache_entries", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "Total number of traces currently stored in the cache from all workers"},
	{Name: "memory_heap_allocation", Type: metrics.Gauge, Unit: metrics.Bytes, Description: "current heap allocation"},
	{Name: "memory_limit", Type: metrics.Gauge, Unit: metrics.Bytes, Description: "configured maximum memory allocation for the collector (derived from MaxAlloc or AvailableMemory * MaxMemoryPercentage)"},
	{Name: "span_received", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans received by the collector"},
	{Name: "span_processed", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans processed by the collector"},
	{Name: "spans_waiting", Type: metrics.UpDown, Unit: metrics.Dimensionless, Description: "number of spans waiting to be processed by the collector"},
	{Name: "trace_sent_cache_hit", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of late spans received for traces that have already been sent"},
	{Name: "trace_accepted", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of new traces received by the collector"},
	{Name: "trace_send_kept", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that has been kept"},
	{Name: "trace_send_dropped", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that has been dropped"},
	{Name: "trace_send_has_root", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of kept traces that have a root span"},
	{Name: "trace_send_no_root", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of kept traces that do not have a root span"},

	{Name: TraceSendGotRoot, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to root span arrival"},
	{Name: TraceSendExpired, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to TraceTimeout or SendDelay"},
	{Name: TraceSendSpanLimit, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to span limit"},
	{Name: TraceSendEjectedFull, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to cache capacity overrun"},
	{Name: TraceSendEjectedMemsize, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of traces that are ready for decision due to memory overrun"},
	{Name: TraceSendLateSpan, Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans that are sent due to late span arrival"},

	{Name: "dropped_from_stress", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans dropped due to stress relief"},
	{Name: "kept_from_stress", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of spans kept due to stress relief"},
	{Name: "events_dropped", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of events dropped"},
	{Name: "trace_kept_sample_rate", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "sample rate of kept traces"},
	{Name: "trace_aggregate_sample_rate", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "aggregate sample rate of both kept and dropped traces"},
	{Name: "collector_collect_loop_duration_ms", Type: metrics.Histogram, Unit: metrics.Milliseconds, Description: "duration of the collect loop, the primary event processing goroutine"},
	{Name: "collector_send_expired_traces_in_cache_dur_ms", Type: metrics.Histogram, Unit: metrics.Milliseconds, Description: "duration of sending expired traces in cache"},
	{Name: "collector_outgoing_queue", Type: metrics.Histogram, Unit: metrics.Dimensionless, Description: "number of traces waiting to be send to upstream"},
	{Name: "collector_cache_eviction", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of times cache eviction has occurred"},
	{Name: "collector_num_workers", Type: metrics.Gauge, Unit: metrics.Dimensionless, Description: "number of collector workers"},
	{Name: "span_counter_id_collision", Type: metrics.Counter, Unit: metrics.Dimensionless, Description: "number of times two spans in the same trace share a span ID while computing scoped SpanCounters"},
}

func (i *InMemCollector) Start() error {
	i.Logger.Debug().Logf("Starting InMemCollector")
	defer func() { i.Logger.Debug().Logf("Finished starting InMemCollector") }()
	imcConfig := i.Config.GetCollectionConfig()

	numWorkers := imcConfig.GetWorkerCount()

	i.Logger.Info().WithField("num_workers", numWorkers).Logf("Starting InMemCollector with %d workers", numWorkers)

	i.StressRelief.UpdateFromConfig()
	i.initSpanCounters()
	// Set queue capacity metrics for stress relief calculations
	i.Metrics.Store(DENOMINATOR_INCOMING_CAP, float64(imcConfig.IncomingQueueSize))
	i.Metrics.Store(DENOMINATOR_PEER_CAP, float64(imcConfig.PeerQueueSize))

	// listen for config reloads
	i.Config.RegisterReloadCallback(i.sendReloadSignal)

	// Find or create a test, make sure we signal health based (somehow)
	// on all the collect workers running.
	i.Health.Register(collectorHealthKey, i.Config.GetHealthCheckTimeout())

	for _, metric := range inMemCollectorMetrics {
		i.Metrics.Register(metric)
	}

	i.tracesToSend = make(chan sendableTrace, 100_000)
	i.done = make(chan struct{})
	i.reload = make(chan struct{}, 1)

	if i.Config.GetAddHostMetadataToTrace() {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			i.hostname = hostname
		}
	}

	// Initialize runtime/metrics sample for efficient memory monitoring
	i.memMetricSample = make([]rtmetrics.Sample, 1)
	i.memMetricSample[0].Name = metrics.RtMetricNameMemory

	i.workers = make([]*CollectorWorker, numWorkers)

	for workerID := range i.workers {
		worker, err := NewCollectorWorker(workerID, i, imcConfig.GetIncomingQueueSizePerWorker(), imcConfig.GetPeerQueueSizePerWorker())
		if err != nil {
			return err
		}
		i.workers[workerID] = worker

		// Start the collect goroutine for this worker
		i.workersWG.Add(1)
		go worker.collect()
	}

	i.sendTracesWG.Add(1)
	go i.sendTraces()

	i.monitorWG.Add(1)
	go i.monitor()

	return nil
}

// sendReloadSignal will trigger the collector reloading its config, eventually.
func (i *InMemCollector) sendReloadSignal(cfgHash, ruleHash string) {
	// non-blocking insert of the signal here so we don't leak goroutines
	select {
	case i.reload <- struct{}{}:
		i.Logger.Debug().Logf("sending collect reload signal")
	default:
		i.Logger.Debug().Logf("collect already waiting to reload; skipping additional signal")
	}
}

func (i *InMemCollector) reloadConfigs() {
	i.Logger.Debug().Logf("reloading in-mem collect config")

	i.SamplerFactory.ClearDynsamplers()

	i.StressRelief.UpdateFromConfig()
	i.initSpanCounters()

	// Send reload signals to all workers to clear their local samplers
	// so that the new configuration will be propagated
	for _, worker := range i.workers {
		select {
		case worker.reload <- struct{}{}:
		default:
			// Channel already has a signal pending, skip
		}
	}
}

// checkAlloc performs memory monitoring using runtime/metrics instead of
// runtime.ReadMemStats. This avoids stop-the-world pauses that can impact performance.
func (i *InMemCollector) checkAlloc(ctx context.Context) {
	inMemConfig := i.Config.GetCollectionConfig()
	maxAlloc := inMemConfig.GetMaxAlloc()
	i.Metrics.Store(DENOMINATOR_MEMORY_MAX_ALLOC, float64(maxAlloc))

	rtmetrics.Read(i.memMetricSample)
	currentAlloc := i.memMetricSample[0].Value.Uint64()

	i.Metrics.Gauge(NUMERATOR_MEMORY_HEAP_ALLOC, float64(currentAlloc))

	// Check if we're within memory budget
	if maxAlloc == 0 || currentAlloc < uint64(maxAlloc) {
		return
	}

	// Memory over budget - trigger cache eviction
	i.Metrics.Increment("collector_cache_eviction")

	// Calculate how much to remove from each worker
	totalToRemove := currentAlloc - uint64(maxAlloc)
	perWorkerToRemove := int(totalToRemove) / len(i.workers)

	// Coordinate eviction across all workers
	var wg sync.WaitGroup
	var cacheSizeBefore, cacheSizeAfter int
	wg.Add(len(i.workers))
	for _, worker := range i.workers {
		cacheSizeBefore += worker.GetCacheSize()
		worker.sendEarly <- sendEarly{
			wg:          &wg,
			bytesToSend: perWorkerToRemove,
		}
	}
	wg.Wait()

	// Calculate actual eviction results
	for _, worker := range i.workers {
		cacheSizeAfter += worker.GetCacheSize()
	}

	// Log the eviction
	i.Logger.Warn().
		WithField("alloc", currentAlloc).
		WithField("old_trace_count", cacheSizeBefore).
		WithField("new_trace_count", cacheSizeAfter).
		Logf("Making some trace decisions early due to memory overrun.")

	// Manually trigger GC to reclaim memory immediately
	runtime.GC()
}

// isReady checks if all workers are healthy by verifying they've
// updated their health status within the configured timeout period.
// Returns true if all workers are healthy, false if any worker is unhealthy.
func (i *InMemCollector) isReady() bool {
	var ready = true // assume ready until proven otherwise
	now := i.Clock.Now()
	timeout := i.Config.GetHealthCheckTimeout()

	for _, worker := range i.workers {
		healthy := worker.IsHealthy(now, timeout)
		if !healthy {
			ready = false
		}
	}

	return ready
}

// monitor runs background maintenance tasks including:
// - Aggregating metrics from all workers
// - Monitoring memory usage and triggering cache eviction
// - Checking worker health and reporting to global health system
// - Handling config reload signals
func (i *InMemCollector) monitor() {
	defer i.monitorWG.Done()

	ctx := context.Background()

	ticker := i.Clock.NewTicker(100 * time.Millisecond)
	i.Health.Ready(collectorHealthKey, true)
	for {
		select {
		case <-ticker.Chan():
			// Check worker health and report aggregated status
			i.Health.Ready(collectorHealthKey, i.isReady())

			// Emit queue capacity limits and memory limit so consumers can compute utilization
			monitorConfig := i.Config.GetCollectionConfig()
			i.Metrics.Gauge("collector_incoming_queue_capacity", float64(monitorConfig.IncomingQueueSize))
			i.Metrics.Gauge("collector_peer_queue_capacity", float64(monitorConfig.PeerQueueSize))
			maxAlloc := monitorConfig.GetMaxAlloc()
			i.Metrics.Gauge("memory_limit", float64(maxAlloc))

			// Aggregate metrics
			totalIncoming := 0
			totalPeer := 0
			totalCacheSize := 0
			var totalWaiting int64
			var totalReceived int64

			for _, worker := range i.workers {
				totalIncoming += len(worker.incoming)
				totalPeer += len(worker.fromPeer)
				totalCacheSize += worker.GetCacheSize()

				totalWaiting += worker.localSpansWaiting.Swap(0)
				totalReceived += worker.localSpanReceived.Swap(0)
			}

			i.Metrics.Histogram("collector_incoming_queue", float64(totalIncoming))
			i.Metrics.Histogram("collector_peer_queue", float64(totalPeer))
			i.Metrics.Histogram("collect_cache_entries", float64(totalCacheSize))
			i.Metrics.Gauge(NUMERATOR_INCOMING_QUEUE, float64(totalIncoming))
			i.Metrics.Gauge(NUMERATOR_PEER_QUEUE, float64(totalPeer))
			i.Metrics.Gauge("collector_cache_size", float64(totalCacheSize))
			i.Metrics.Gauge("collector_num_workers", float64(len(i.workers)))

			// Report aggregated thread-local metrics
			i.Metrics.Count("span_received", totalReceived)
			i.Metrics.Count("spans_waiting", totalWaiting)

			// Check memory and evict if needed (using runtime/metrics - no STW)
			i.checkAlloc(ctx)
		case <-i.reload:
			i.reloadConfigs()
		case <-i.done:
			ticker.Stop()
			return
		}
	}
}

// getWorkerIDForTrace determines which CollectorWorker should handle a given trace ID
// using consistent hashing to ensure all spans for a trace go to the same worker
func (i *InMemCollector) getWorkerIDForTrace(traceID string) int {
	// Hash with a seed so that we don't align with any other hashes of this
	// trace. We use a different algorithm to assign traces to nodes, but we
	// still want to minimize the risk of any synchronization between that
	// distribution and this one.
	hash := wyhash.Hash([]byte(traceID), 7215963184435617557)

	// Map to worker index
	workerIndex := int(hash % uint64(len(i.workers)))

	return workerIndex
}

// AddSpan accepts the incoming span to a queue and returns immediately
func (i *InMemCollector) AddSpan(sp *types.Span) error {
	// Route to the appropriate worker
	workerIndex := i.getWorkerIDForTrace(sp.TraceID)
	return i.workers[workerIndex].addSpan(sp)
}

// AddSpanFromPeer accepts the incoming span from a peer to a queue and returns immediately
func (i *InMemCollector) AddSpanFromPeer(sp *types.Span) error {
	// Route to the appropriate worker
	workerIndex := i.getWorkerIDForTrace(sp.TraceID)
	return i.workers[workerIndex].addSpanFromPeer(sp)
}

// Stressed returns true if the collector is undergoing significant stress
func (i *InMemCollector) Stressed() bool {
	return i.StressRelief.Stressed()
}

func (i *InMemCollector) GetStressedSampleRate(traceID string) (rate uint, keep bool, reason string) {
	return i.StressRelief.GetSampleRate(traceID)
}

// ProcessSpanImmediately is an escape hatch used under stressful conditions --
// it submits a span for immediate transmission without enqueuing it for normal
// processing. This means it ignores dry run mode and doesn't build a complete
// trace context or cache the trace in the active trace buffer. It only gets
// called on the first span for a trace under stressful conditions; we got here
// because the StressRelief system detected that this is a new trace AND that it
// is being sampled. Therefore, we also put the traceID into the sent traces
// cache as "kept".
// It doesn't do any logging and barely touches metrics; this is about as
// minimal as we can make it.
func (i *InMemCollector) ProcessSpanImmediately(sp *types.Span) (processed bool, keep bool) {
	_, span := otelutil.StartSpanWith(context.Background(), i.Tracer, "collector.ProcessSpanImmediately", "trace_id", sp.TraceID)
	defer span.End()

	// Find which worker owns this trace
	workerIndex := i.getWorkerIDForTrace(sp.TraceID)
	worker := i.workers[workerIndex]

	var rate uint
	record, reason, found := worker.sampleCache.CheckSpan(sp)
	if !found {
		rate, keep, reason = i.StressRelief.GetSampleRate(sp.TraceID)
		now := i.Clock.Now()
		trace := &types.Trace{
			APIHost:     sp.APIHost,
			APIKey:      sp.APIKey,
			Dataset:     sp.Dataset,
			TraceID:     sp.TraceID,
			ArrivalTime: now,
			SendBy:      now,
		}
		trace.SetSampleRate(rate)
		// we do want a record of how we disposed of traces in case more come in after we've
		// turned off stress relief (if stress relief is on we'll keep making the same decisions)
		worker.sampleCache.Record(trace, keep, reason)
	} else {
		rate = record.Rate()
		keep = record.Kept()
	}

	if !keep {
		i.Metrics.Increment("dropped_from_stress")
		i.Metrics.Increment("events_dropped")
		return true, false
	}

	i.Metrics.Increment("kept_from_stress")
	// ok, we're sending it, so decorate it first
	sp.Data.Set(types.MetaStressed, true)
	if i.Config.GetAddRuleReasonToTrace() {
		sp.Data.Set(types.MetaRefineryReason, reason)
	}
	if i.hostname != "" {
		sp.Data.Set(types.MetaRefineryLocalHostname, i.hostname)
	}

	i.addAdditionalAttributes(sp)
	mergeTraceAndSpanSampleRates(sp, rate, i.Config.GetIsDryRun())
	i.Transmission.EnqueueSpan(sp)

	return true, true
}

// dealWithSentTrace handles a span that has arrived after the sampling decision
// on the trace has already been made, and it obeys that decision by either
// sending the span immediately or dropping it.
// This method is made public so CollectWorker can access it.
func (i *InMemCollector) dealWithSentTrace(ctx context.Context, tr cache.TraceSentRecord, keptReason string, sp *types.Span) {
	_, span := otelutil.StartSpanMulti(ctx, i.Tracer, "dealWithSentTrace", map[string]interface{}{
		"trace_id":    sp.TraceID,
		"kept_reason": keptReason,
		"hostname":    i.hostname,
	})
	defer span.End()

	if i.Config.GetAddRuleReasonToTrace() {
		var metaReason string
		if len(keptReason) > 0 {
			metaReason = fmt.Sprintf("%s - late arriving span", keptReason)
		} else {
			metaReason = "late arriving span"
		}
		sp.Data.Set(types.MetaRefineryReason, metaReason)
		sp.Data.Set(types.MetaRefinerySendReason, TraceSendLateSpan)

	}
	if i.hostname != "" {
		sp.Data.Set(types.MetaRefineryLocalHostname, i.hostname)
	}
	isDryRun := i.Config.GetIsDryRun()
	keep := tr.Kept()
	otelutil.AddSpanFields(span, map[string]interface{}{
		"keep":      keep,
		"is_dryrun": isDryRun,
	})

	if isDryRun {
		// if dry run mode is enabled, we keep all traces and mark the spans with the sampling decision
		sp.Data.Set(config.DryRunFieldName, keep)
		if !keep {
			i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Sending span that would have been dropped, but dry run mode is enabled")
			i.Metrics.Increment(TraceSendLateSpan)
			i.addAdditionalAttributes(sp)
			i.Transmission.EnqueueSpan(sp)
			return
		}
	}
	if keep {
		i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Sending span because of previous decision to send trace")
		mergeTraceAndSpanSampleRates(sp, tr.Rate(), isDryRun)
		// if this span is a late root span, possibly update it with our current span count
		if sp.IsRoot {
			if i.Config.GetAddCountsToRoot() {
				sp.Data.Set(types.MetaSpanEventCount, int64(tr.SpanEventCount()))
				sp.Data.Set(types.MetaSpanLinkCount, int64(tr.SpanLinkCount()))
				sp.Data.Set(types.MetaSpanCount, int64(tr.SpanCount()))
				sp.Data.Set(types.MetaEventCount, int64(tr.DescendantCount()))
			} else if i.Config.GetAddSpanCountToRoot() {
				sp.Data.Set(types.MetaSpanCount, int64(tr.DescendantCount()))
			}
		}
		otelutil.AddSpanField(span, "is_root_span", sp.IsRoot)
		i.Metrics.Increment(TraceSendLateSpan)
		i.addAdditionalAttributes(sp)
		i.Transmission.EnqueueSpan(sp)
		return
	}
	i.Metrics.Increment("events_dropped")
	i.Logger.Debug().WithField("trace_id", sp.TraceID).Logf("Dropping span because of previous decision to drop trace")
}

func mergeTraceAndSpanSampleRates(sp *types.Span, traceSampleRate uint, dryRunMode bool) {
	tempSampleRate := sp.SampleRate
	if sp.SampleRate != 0 {
		// Write down the original sample rate so that that information
		// is more easily recovered
		sp.Data.Set(types.MetaRefineryOriginalSampleRate, int64(sp.SampleRate))
	}

	if tempSampleRate < 1 {
		// See https://docs.honeycomb.io/manage-data-volume/sampling/
		// SampleRate is the denominator of the ratio of sampled spans
		// HoneyComb treats a missing or 0 SampleRate the same as 1, but
		// behaves better/more consistently if the SampleRate is explicitly
		// set instead of inferred
		tempSampleRate = 1
	}

	// if spans are already sampled, take that into account when computing
	// the final rate
	if dryRunMode {
		sp.Data.Set("meta.dryrun.sample_rate", tempSampleRate*traceSampleRate)
		sp.SampleRate = tempSampleRate
	} else {
		finalSampleRate := tempSampleRate * traceSampleRate
		sp.SampleRate = finalSampleRate
		sp.Data.Set(types.MetaRefineryFinalSampleRate, int64(finalSampleRate))
	}
}

// this is only called when a trace decision is received
// TODO it may be desirable to move this and sendTraes() into the CollectWorker.
func (i *InMemCollector) send(ctx context.Context, trace sendableTrace) {
	if trace.Sent {
		// someone else already sent this so we shouldn't also send it.
		i.Logger.Debug().
			WithString("trace_id", trace.TraceID).
			WithString("dataset", trace.Dataset).
			Logf("skipping send because someone else already sent trace to dataset")
		return
	}
	trace.Sent = true
	_, span := otelutil.StartSpan(ctx, i.Tracer, "send")
	defer span.End()

	traceDur := i.Clock.Since(trace.ArrivalTime)
	i.Metrics.Histogram("trace_duration_ms", float64(traceDur.Milliseconds()))

	logFields := logrus.Fields{
		"trace_id": trace.TraceID,
	}
	// if we're supposed to drop this trace, and dry run mode is not enabled, then we're done.
	if !trace.KeepSample && !i.Config.GetIsDryRun() {
		i.Metrics.Increment("trace_send_dropped")
		dropCount := int64(trace.DescendantCount())
		i.Metrics.Count("events_dropped", dropCount)
		i.Logger.Debug().WithFields(logFields).Logf("Dropping trace because of sampling decision")
		return
	}

	if trace.RootSpan != nil {
		rs := trace.RootSpan
		if rs != nil {
			if i.Config.GetAddCountsToRoot() {
				rs.Data.Set(types.MetaSpanEventCount, int64(trace.SpanEventCount()))
				rs.Data.Set(types.MetaSpanLinkCount, int64(trace.SpanLinkCount()))
				rs.Data.Set(types.MetaSpanCount, int64(trace.SpanCount()))
				rs.Data.Set(types.MetaEventCount, int64(trace.DescendantCount()))
			} else if i.Config.GetAddSpanCountToRoot() {
				rs.Data.Set(types.MetaSpanCount, int64(trace.DescendantCount()))
			}
		}
	}

	i.Metrics.Increment(trace.sendReason)
	if config.IsLegacyAPIKey(trace.APIKey) {
		logFields["dataset"] = trace.samplerSelector
	} else {
		logFields["environment"] = trace.samplerSelector
	}
	logFields["reason"] = trace.reason
	if trace.sampleKey != "" {
		logFields["sample_key"] = trace.sampleKey
	}

	i.Metrics.Increment("trace_send_kept")
	// This will observe sample rate decisions only if the trace is kept
	i.Metrics.Histogram("trace_kept_sample_rate", float64(trace.Trace.SampleRate()))

	// ok, we're not dropping this trace; send all the spans
	if i.Config.GetIsDryRun() && !trace.shouldSend {
		i.Logger.Debug().WithFields(logFields).Logf("Trace would have been dropped, but sending because dry run mode is enabled")
	} else {
		i.Logger.Debug().WithFields(logFields).Logf("Sending trace")
	}

	i.tracesToSend <- trace
}

func (i *InMemCollector) Stop() error {
	i.Logger.Debug().Logf("Starting InMemCollector shutdown")

	// Signal shutdown to all components
	close(i.done)

	// signal the health system to not be ready and
	// stop liveness check so that no new traces are accepted
	i.Health.Unregister(collectorHealthKey)

	// Stop maintenance first - we want to make sure we don't start a checkAlloc
	// after shutting down the workers.
	i.monitorWG.Wait()

	// Close all worker input channels, which will cause the workers to stop.
	for idx, worker := range i.workers {
		i.Logger.Debug().WithField("worker_id", idx).Logf("closing worker channels")
		close(worker.incoming)
		close(worker.fromPeer)
	}
	i.workersWG.Wait()

	for _, worker := range i.workers {
		worker.Stop()
	}

	// Now it's safe to close the traces to send channel
	// No more traces will be sent to it
	close(i.tracesToSend)
	i.sendTracesWG.Wait()

	i.Logger.Debug().Logf("InMemCollector shutdown complete")
	return nil
}

// sentRecord is a struct that holds a span and the record of the trace decision made.
type sentRecord struct {
	span   *types.Span
	record cache.TraceSentRecord
	reason string
}

func (i *InMemCollector) addAdditionalAttributes(sp *types.Span) {
	for k, v := range i.Config.GetAdditionalAttributes() {
		sp.Data.Set(k, v)
	}
}

// initSpanCounters loads and initializes span counters from the current config.
// Must be called at startup and on config reload.
func (i *InMemCollector) initSpanCounters() {
	counters := i.Config.GetSpanCounters()
	for j := range counters {
		if err := counters[j].Init(); err != nil {
			i.Logger.Error().WithField("error", err).Logf("failed to initialize span counter %q", counters[j].Key)
		}
	}
	i.mutex.Lock()
	i.spanCounters = counters
	i.mutex.Unlock()
}

// findSuitableRootSpan returns the root span of the trace if one is present.
// If no root span has been identified, it falls back to the non-annotation
// span (i.e. not a span event or link) with the earliest timestamp, which is
// the most likely root. Returns nil if no suitable span exists.
func findSuitableRootSpan(t sendableTrace) *types.Span {
	if t.RootSpan != nil {
		return t.RootSpan
	}
	var best *types.Span
	for _, sp := range t.GetSpans() {
		if sp.AnnotationType() != types.SpanAnnotationTypeSpanEvent &&
			sp.AnnotationType() != types.SpanAnnotationTypeLink {
			if best == nil || sp.Timestamp.Before(best.Timestamp) {
				best = sp
			}
		}
	}
	return best
}

// customCountWrite is a single counter-keyed write destined for one span.
type customCountWrite struct {
	key   string
	value int64
}

// computeCustomCounts computes each configured SpanCounter and returns the
// per-span attribute writes the caller should apply.
//
// Returns nil if there are no counters configured or no spans to count.
//
// Fast path: when no counter has ScopeConditions, run a single linear scan
// over the spans and write the trace-wide total to the root span — identical
// to the original behavior, with no index, DFS, or per-span storage.
//
// Scoped path (engaged when at least one counter has ScopeConditions): build a
// parent->children index, run iterative post-order DFS from each forest root
// (and any unvisited orphan island) to compute per-span subtree counts, then
// emit per-anchor writes plus an optional trace-wide total on the root.
//
// Stress relief note: this runs inside sendTraces(), the sole consumer of the
// tracesToSend channel. Work is O(N×M) — N spans × M counters — so large
// traces with many counters slow the consumer, which deepens the outgoing
// queue. The stress relief system monitors queue depth as one of its stress
// inputs, so heavy custom-count configurations can raise the measured stress
// level and trigger earlier activation of stress relief. Additionally, spans
// processed via ProcessSpanImmediately (the stress-relief fast path) bypass the
// trace buffer entirely and never reach sendTraces, so custom counts are not
// computed or attached to stress-sampled traces.
func (i *InMemCollector) computeCustomCounts(t sendableTrace) map[*types.Span][]customCountWrite {
	i.mutex.RLock()
	counters := i.spanCounters
	i.mutex.RUnlock()

	if len(counters) == 0 {
		return nil
	}

	spans := t.GetSpans()
	if len(spans) == 0 {
		return nil
	}

	rootSpan := findSuitableRootSpan(t)
	var rootData config.SpanData
	if rootSpan != nil {
		rootData = &rootSpan.Data
	}

	anyScoped := false
	for _, c := range counters {
		if len(c.ScopeConditions) > 0 {
			anyScoped = true
			break
		}
	}
	if !anyScoped {
		return computeCustomCountsLinear(spans, counters, rootSpan, rootData)
	}

	spanIDFields := i.Config.GetSpanIdFieldNames()
	parentIDFields := i.Config.GetParentIdFieldNames()
	memoFields := make([]string, 0, len(spanIDFields)+len(parentIDFields))
	memoFields = append(memoFields, spanIDFields...)
	memoFields = append(memoFields, parentIDFields...)
	for _, sp := range spans {
		sp.Data.MemoizeFields(memoFields...)
	}

	childrenByIndex, forestRoots := buildSpanIndex(spans, spanIDFields, parentIDFields, i.Metrics)

	M := len(counters)
	counts := make([]int64, len(spans)*M)
	visited := make([]bool, len(spans))

	for _, ri := range forestRoots {
		aggregateSubtree(ri, spans, childrenByIndex, counters, rootData, counts, visited, M)
	}
	for idx := range visited {
		if !visited[idx] {
			aggregateSubtree(idx, spans, childrenByIndex, counters, rootData, counts, visited, M)
		}
	}

	emissions := make(map[*types.Span][]customCountWrite)
	for c, counter := range counters {
		if len(counter.ScopeConditions) > 0 {
			for si, sp := range spans {
				if counter.MatchesScope(&sp.Data, rootData) {
					emissions[sp] = append(emissions[sp], customCountWrite{counter.Key, counts[si*M+c]})
				}
			}
		}
		if counter.ShouldEmitTotalOnRoot() && rootSpan != nil {
			var total int64
			if len(counter.ScopeConditions) > 0 {
				for _, ri := range forestRoots {
					total += counts[ri*M+c]
				}
			} else {
				for si := range spans {
					total += counts[si*M+c]
				}
			}
			emissions[rootSpan] = append(emissions[rootSpan], customCountWrite{counter.EffectiveRootKey(), total})
		}
	}

	return emissions
}

// computeCustomCountsLinear implements the unscoped fast path: a single linear
// pass over spans accumulating one int64 per counter, written to the root.
func computeCustomCountsLinear(spans []*types.Span, counters []config.SpanCounter, rootSpan *types.Span, rootData config.SpanData) map[*types.Span][]customCountWrite {
	if rootSpan == nil {
		return nil
	}
	totals := make([]int64, len(counters))
	for _, sp := range spans {
		for c, counter := range counters {
			if counter.MatchesSpan(&sp.Data, rootData) {
				totals[c]++
			}
		}
	}
	writes := make([]customCountWrite, 0, len(counters))
	for c, counter := range counters {
		writes = append(writes, customCountWrite{counter.Key, totals[c]})
	}
	return map[*types.Span][]customCountWrite{rootSpan: writes}
}

// spanIDFromPayload reads the first present configured ID field and narrows
// it to a string. Returns ("", false) for missing/empty IDs and for types
// other than string/[]byte — such spans become leaf-only in the index.
func spanIDFromPayload(p config.SpanData, fields []string) (string, bool) {
	for _, f := range fields {
		if !p.Exists(f) {
			continue
		}
		switch v := p.Get(f).(type) {
		case string:
			if v == "" {
				return "", false
			}
			return v, true
		case []byte:
			if len(v) == 0 {
				return "", false
			}
			return string(v), true
		}
		return "", false
	}
	return "", false
}

// buildSpanIndex constructs the parent->children index used by the scoped
// aggregation pass. childrenByIndex[i] holds the span indices of span i's
// children, indexed for O(1) DFS lookups (no string keys in the hot path).
// forestRoots holds the indices of spans with no parent or whose parent ID
// is not present in this trace; self-loops are also routed through
// forestRoots so the cycle-defense pass guarantees visitation. Collisions
// on span ID are logged via the span_counter_id_collision metric and
// resolved last-write-wins.
func buildSpanIndex(spans []*types.Span, spanIDFields, parentIDFields []string, m metrics.Metrics) ([][]int, []int) {
	idToIndex := make(map[string]int, len(spans))
	for i, sp := range spans {
		id, ok := spanIDFromPayload(&sp.Data, spanIDFields)
		if !ok {
			continue
		}
		if _, exists := idToIndex[id]; exists {
			if m != nil {
				m.Increment("span_counter_id_collision")
			}
		}
		idToIndex[id] = i
	}

	childrenByIndex := make([][]int, len(spans))
	var forestRoots []int
	for i, sp := range spans {
		parentID, parentOk := spanIDFromPayload(&sp.Data, parentIDFields)
		if !parentOk {
			forestRoots = append(forestRoots, i)
			continue
		}
		parentIdx, parentInTrace := idToIndex[parentID]
		if !parentInTrace || parentIdx == i {
			forestRoots = append(forestRoots, i)
			continue
		}
		childrenByIndex[parentIdx] = append(childrenByIndex[parentIdx], i)
	}
	return childrenByIndex, forestRoots
}

// aggregateSubtree runs iterative post-order DFS from rootIndex, populating
// counts[span*M+c] with each counter's subtree count (children's sums plus 1
// for each counter whose Conditions match the span itself). visited gates
// re-entry so cycles terminate; M is the per-span counter stride.
func aggregateSubtree(
	rootIndex int,
	spans []*types.Span,
	childrenByIndex [][]int,
	counters []config.SpanCounter,
	rootData config.SpanData,
	counts []int64,
	visited []bool,
	M int,
) {
	if visited[rootIndex] {
		return
	}
	type frame struct {
		spanIndex   int
		childCursor int
	}
	stack := []frame{{spanIndex: rootIndex, childCursor: 0}}
	visited[rootIndex] = true

	for len(stack) > 0 {
		top := &stack[len(stack)-1]
		children := childrenByIndex[top.spanIndex]
		if top.childCursor < len(children) {
			childIdx := children[top.childCursor]
			top.childCursor++
			if visited[childIdx] {
				continue
			}
			visited[childIdx] = true
			stack = append(stack, frame{spanIndex: childIdx, childCursor: 0})
			continue
		}

		sp := spans[top.spanIndex]
		base := top.spanIndex * M
		for c, counter := range counters {
			if counter.MatchesSpan(&sp.Data, rootData) {
				counts[base+c] = 1
			}
		}
		for _, childIdx := range children {
			cbase := childIdx * M
			for c := range counters {
				counts[base+c] += counts[cbase+c]
			}
		}
		stack = stack[:len(stack)-1]
	}
}

func (i *InMemCollector) sendTraces() {
	defer i.sendTracesWG.Done()

	for t := range i.tracesToSend {
		i.Metrics.Histogram("collector_outgoing_queue", float64(len(i.tracesToSend)))
		_, span := otelutil.StartSpanMulti(context.Background(), i.Tracer, "sendTrace", map[string]interface{}{"num_spans": t.DescendantCount(), "tracesToSend_size": len(i.tracesToSend)})

		customCounts := i.computeCustomCounts(t)

		for _, sp := range t.GetSpans() {

			if i.Config.GetAddRuleReasonToTrace() {
				sp.Data.Set(types.MetaRefineryReason, t.reason)
				sp.Data.Set(types.MetaRefinerySendReason, t.sendReason)
				if t.sampleKey != "" {
					sp.Data.Set(types.MetaRefinerySampleKey, t.sampleKey)
				}
			}

			// update the root span (if we have one, which we might not if the trace timed out)
			// with the final total as of our send time
			if sp.IsRoot {
				if i.Config.GetAddCountsToRoot() {
					sp.Data.Set(types.MetaSpanEventCount, int64(t.SpanEventCount()))
					sp.Data.Set(types.MetaSpanLinkCount, int64(t.SpanLinkCount()))
					sp.Data.Set(types.MetaSpanCount, int64(t.SpanCount()))
					sp.Data.Set(types.MetaEventCount, int64(t.DescendantCount()))
				} else if i.Config.GetAddSpanCountToRoot() {
					sp.Data.Set(types.MetaSpanCount, int64(t.DescendantCount()))
				}
			}

			for _, w := range customCounts[sp] {
				sp.Data.Set(w.key, w.value)
			}

			isDryRun := i.Config.GetIsDryRun()
			if isDryRun {
				sp.Data.Set(config.DryRunFieldName, t.shouldSend)
			}
			if i.hostname != "" {
				sp.Data.Set(types.MetaRefineryLocalHostname, i.hostname)
			}
			mergeTraceAndSpanSampleRates(sp, t.SampleRate(), isDryRun)
			i.addAdditionalAttributes(sp)

			sp.APIKey = t.APIKey
			i.Transmission.EnqueueSpan(sp)
		}
		span.End()
	}
}

func (i *InMemCollector) IsMyTrace(traceID string) (sharder.Shard, bool) {
	// if trace locality is disabled, we should only process
	// traces that belong to the current refinery
	targeShard := i.Sharder.WhichShard(traceID)

	return targeShard, i.Sharder.MyShard().Equals(targeShard)

}
