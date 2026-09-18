package gate

import (
	"fmt"
	"strconv"
	"time"
)

// ai-generated: the whole file (the metric keys and the verdict over them).

// Metric keys: the names scenarios record under, and the verdict and the
// report read. Times are in milliseconds, rates in bits per second, memory in
// bytes; a flag is 1 for yes.
const (
	MetricHandshakeMs       = "handshake_ms"        // S0: client start to a working tunnel
	MetricPullOK            = "pull_ok"             // S0 flag; S2: pulls that completed
	MetricPullTotal         = "pull_total"          // S2: pulls started
	MetricPushOK            = "push_ok"             // S0 flag; S3: pushes that completed
	MetricPushTotal         = "push_total"          // S3: pushes started
	MetricConnectOK         = "connect_ok"          // S1: connects that fetched the small resource
	MetricConnectTotal      = "connect_total"       // S1: connects tried
	MetricConnectP95Ms      = "connect_p95_ms"      // S1: 95th percentile of a connect
	MetricThroughputDownBps = "throughput_down_bps" // S2: aggregate download rate
	MetricThroughputUpBps   = "throughput_up_bps"   // S3: aggregate upload rate
	MetricOnTopOK           = "on_top_ok"           // S2, S3: connects on top of the load that worked
	MetricOnTopTotal        = "on_top_total"        // S2, S3: connects on top of the load tried
	MetricOnTopP95Ms        = "on_top_p95_ms"       // S2, S3: 95th percentile of a connect on top
	MetricAlive             = "alive"               // S4 flag: the control session lived through the quiet
	MetricMissedPong        = "missed_pong"         // S4: missed pongs logged in the quiet
	MetricReconnects        = "reconnects"          // S4: client reconnects in the quiet
	MetricFinalPullOK       = "final_pull_ok"       // S4 flag: the pull after the quiet worked
	MetricAnswered1         = "answered_1"          // S5: queries of the first burst answered
	MetricAnswered2         = "answered_2"          // S5: queries of the second burst answered
	MetricReady3sMs         = "ready_3s_ms"         // S6: ready with the bridge 3 s late, 0 if never
	MetricReady8sMs         = "ready_8s_ms"         // S6: ready with the bridge 8 s late, 0 if never
	MetricHeapPeakBytes     = "heap_peak_bytes"     // S7: peak live heap over S2-S4
	MetricRSSPeakBytes      = "rss_peak_bytes"      // S7: peak RSS over S2-S4
	MetricGoroutinesIdle    = "goroutines_idle"     // S7: goroutines before the load
	MetricGoroutinesAfter   = "goroutines_after"    // S7: goroutines 60 s after the load
)

// Evaluate judges a scenario's metrics. An empty result is a pass; every
// string names the metric that missed and both numbers, so the report reads.
// A scenario without rules fails: nothing passes unjudged.
func Evaluate(scenario string, m Metrics, t Thresholds) []string {
	rules := rulesFor(scenario, t)
	if rules == nil {
		return []string{"no rules for scenario " + scenario}
	}
	var failures []string
	for _, r := range rules {
		if f := r(m); f != "" {
			failures = append(failures, f)
		}
	}
	return failures
}

// rule is one pass criterion: what missed, or "" when the metrics meet it.
type rule func(m Metrics) string

// rulesFor lists a scenario's pass criteria (spec section 4) in the order a
// report reads what missed; nil for a scenario it does not know.
func rulesFor(scenario string, t Thresholds) []rule {
	p95 := ms(t.ConnectP95)
	switch scenario {
	case "S0":
		return []rule{atMost(MetricHandshakeMs, ms(handshakeBudget)), isOne(MetricPullOK), isOne(MetricPushOK)}
	case "S1":
		return []rule{allOf(MetricConnectOK, MetricConnectTotal), atMost(MetricConnectP95Ms, p95)}
	case "S2":
		return []rule{allOf(MetricPullOK, MetricPullTotal), atLeast(MetricThroughputDownBps, t.ThroughputDownBps),
			allOf(MetricOnTopOK, MetricOnTopTotal), atMost(MetricOnTopP95Ms, p95)}
	case "S3":
		return []rule{allOf(MetricPushOK, MetricPushTotal), atLeast(MetricThroughputUpBps, t.ThroughputUpBps),
			allOf(MetricOnTopOK, MetricOnTopTotal), atMost(MetricOnTopP95Ms, p95)}
	case "S4":
		return []rule{isOne(MetricAlive), atMost(MetricMissedPong, 0), atMost(MetricReconnects, 0),
			isOne(MetricFinalPullOK)}
	case "S5":
		floor := float64(t.ResolverAnswered)
		return []rule{atLeast(MetricAnswered1, floor), atLeast(MetricAnswered2, floor)}
	case "S6":
		return []rule{readyWithin(MetricReady3sMs, 3*time.Second), readyWithin(MetricReady8sMs, 8*time.Second)}
	case "S7":
		return []rule{atMost(MetricHeapPeakBytes, t.HeapPeakBytes), atMost(MetricRSSPeakBytes, t.RSSPeakBytes),
			goroutineGrowth(t.GoroutineGrowth)}
	}
	return nil
}

// atMost is a ceiling. The metric must have been recorded: read as zero, one
// a scenario never measured would pass. NaN fails too, as no comparison holds.
func atMost(key string, limit float64) rule {
	return func(m Metrics) string {
		v, ok := m[key]
		switch {
		case !ok:
			return notMeasured(key)
		case v <= limit:
			return ""
		}
		return breach(key, v, ">", limit)
	}
}

// atLeast is a floor. A metric never recorded reads as zero and fails it, and
// so does NaN.
func atLeast(key string, floor float64) rule {
	return func(m Metrics) string {
		v := m[key]
		if v >= floor {
			return ""
		}
		return breach(key, v, "<", floor)
	}
}

// isOne is a flag a scenario sets to 1 when a step worked; one it left unset
// reads as zero and fails.
func isOne(key string) rule {
	return func(m Metrics) string {
		if v := m[key]; v != 1 {
			return breach(key, v, "!=", 1)
		}
		return ""
	}
}

// allOf wants every attempt to have worked, and at least one attempt: 0 of 0
// is a scenario that did nothing.
func allOf(okKey, totalKey string) rule {
	return func(m Metrics) string {
		done, total := m[okKey], m[totalKey]
		if done == total && total > 0 {
			return ""
		}
		return okKey + " " + num(done) + " of " + totalKey + " " + num(total)
	}
}

// readyWithin is S6's: the client got ready at all (a scenario records 0 when
// it never did), and within the delay the server's bridge opened with plus
// the handshake budget.
func readyWithin(key string, delay time.Duration) rule {
	limit := ms(delay + handshakeBudget)
	return func(m Metrics) string {
		v := m[key]
		switch {
		case v > 0 && v <= limit:
			return ""
		case v > 0:
			return breach(key, v, ">", limit)
		}
		return key + " " + num(v) + ": never ready"
	}
}

// goroutineGrowth bounds the goroutines a client still runs 60 s after the
// load over its idle baseline. Both counts must have been recorded: growth
// from a missing one is a number nobody measured.
func goroutineGrowth(limit int) rule {
	return func(m Metrics) string {
		idle, hasIdle := m[MetricGoroutinesIdle]
		after, hasAfter := m[MetricGoroutinesAfter]
		switch {
		case !hasIdle:
			return notMeasured(MetricGoroutinesIdle)
		case !hasAfter:
			return notMeasured(MetricGoroutinesAfter)
		}
		growth := after - idle
		if growth <= float64(limit) {
			return ""
		}
		return fmt.Sprintf("goroutines grew by %s > %d (%s %s, %s %s)", num(growth), limit,
			MetricGoroutinesIdle, num(idle), MetricGoroutinesAfter, num(after))
	}
}

// notMeasured words a ceiling whose metric a scenario never recorded.
func notMeasured(key string) string { return key + " not measured" }

// breach words a bound a metric missed: the metric, what it measured, the
// relation that failed and the bound.
func breach(key string, v float64, rel string, bound float64) string {
	return key + " " + num(v) + " " + rel + " " + num(bound)
}

// num prints a metric the way a person counts: 2000000, not 2e+06.
func num(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
