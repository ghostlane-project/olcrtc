package gate

import "time"

// ai-generated: the whole file (every number the verdicts judge by).

// Thresholds are the numbers a target's verdicts judge by. They live in this
// file and nowhere else (spec section 6): change them here.
type Thresholds struct {
	// ConnectP95 bounds the 95th percentile of a connect through the tunnel,
	// idle or with bulk transfer in flight (the ack deadline is the same order).
	ConnectP95 time.Duration
	// ThroughputDownBps and ThroughputUpBps are aggregate floors for the
	// saturation scenarios: half of what the harness measured, so runner
	// variance does not fail a good build.
	ThroughputDownBps float64
	ThroughputUpBps   float64
	// HeapPeakBytes and RSSPeakBytes bound the mobile flavour over S2-S4
	// under the phone's memory limit and GC settings.
	HeapPeakBytes float64
	RSSPeakBytes  float64
	// GoroutineGrowth bounds goroutines 60 s after load versus idle.
	GoroutineGrowth int
	// ResolverAnswered is how many of 64 burst queries must be answered.
	ResolverAnswered int
}

// handshakeBudget bounds a handshake on every target: S0's, and each of S6's
// on top of the delay its server's bridge opens with. It equals the engine's
// reply deadline (handshake.DefaultTimeout) but is written out, so a change
// to the engine cannot loosen the gate.
const handshakeBudget = 15 * time.Second

// Local is the target with the server in the runner: Jitsi's relay carries
// ~5 Mbit/s, WB Stream and Telemost a few Mbit/s.
var Local = Thresholds{ //nolint:gochecknoglobals // the one place these numbers live
	ConnectP95:        5 * time.Second,
	ThroughputDownBps: 2_000_000,
	ThroughputUpBps:   2_000_000,
	HeapPeakBytes:     16 << 20,
	RSSPeakBytes:      45 << 20,
	GoroutineGrowth:   20,
	ResolverAnswered:  63,
}

// Link is the fleet's DE node through Telemost, measured at 1.4-1.6 Mbit/s
// down and ~3 Mbit/s up from a datacenter.
var Link = Thresholds{ //nolint:gochecknoglobals // the one place these numbers live
	ConnectP95:        5 * time.Second,
	ThroughputDownBps: 800_000,
	ThroughputUpBps:   1_500_000,
	HeapPeakBytes:     16 << 20,
	RSSPeakBytes:      45 << 20,
	GoroutineGrowth:   20,
	ResolverAnswered:  63,
}

// Names in Map of the two knobs that bound no single metric. The others are
// named after the metric they bound, so a cell shows a bound next to its value.
const (
	thresholdGoroutineGrowth  = "goroutine_growth"
	thresholdResolverAnswered = "resolver_answered"
)

// Map is what a cell records next to its metrics, times in milliseconds as
// the metrics carry them.
func (t Thresholds) Map() map[string]float64 {
	return map[string]float64{
		MetricConnectP95Ms:        ms(t.ConnectP95),
		MetricThroughputDownBps:   t.ThroughputDownBps,
		MetricThroughputUpBps:     t.ThroughputUpBps,
		MetricHeapPeakBytes:       t.HeapPeakBytes,
		MetricRSSPeakBytes:        t.RSSPeakBytes,
		thresholdGoroutineGrowth:  float64(t.GoroutineGrowth),
		thresholdResolverAnswered: float64(t.ResolverAnswered),
	}
}

// ms is a duration in milliseconds, the unit the metrics carry time in.
func ms(d time.Duration) float64 { return float64(d.Milliseconds()) }
