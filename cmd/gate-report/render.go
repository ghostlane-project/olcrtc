package main

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/openlibrecommunity/olcrtc/internal/gate"
)

// ai-generated: the whole file (the Markdown table of one report).

// Render writes the report as the table the job summary and the release show.
func Render(r gate.Report) string {
	heading := fmt.Sprintf("### Gate: %s target, engine %s", r.Target, cmp.Or(short(r.EngineCommit), "unknown"))
	if r.AppVersion != "" {
		heading += ", app " + r.AppVersion
	}
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "%s\n\n%s\n\n| Cell | Verdict | Key metrics | Took |\n| --- | --- | --- | --- |\n",
		heading, summary(r))
	cells := slices.Clone(r.Cells)
	slices.SortFunc(cells, func(a, b gate.Cell) int { return cmp.Compare(a.ID, b.ID) })
	for _, c := range cells {
		_, _ = fmt.Fprintf(&b, "| `%s` | %s | %s | %.0f s |\n", c.ID, verdict(c), keyMetrics(c), c.DurationS)
	}
	return b.String()
}

// summary is the line under the heading: how the cells ended, the runner and
// how long the run took. A report with no cells is not a pass.
func summary(r gate.Report) string {
	outcome := fmt.Sprintf("%d of %d cells failed", r.Failed, r.Planned)
	switch {
	case r.Planned == 0:
		outcome = "no cells were planned"
	case r.Failed == 0:
		outcome = fmt.Sprintf("all %d cells passed", r.Planned)
	}
	parts := []string{outcome}
	if r.Runner != "" {
		parts = append(parts, r.Runner)
	}
	parts = append(parts, fmt.Sprintf("%.0f s", r.DurationS))
	return strings.Join(parts, " · ")
}

// verdict is a pass mark, or a failure mark with what missed. Anything but a
// pass is a failure: a report holds no skipped cell.
func verdict(c gate.Cell) string {
	if c.Status == gate.StatusPass {
		return "✅"
	}
	return strings.TrimSpace("❌ " + cellText(strings.Join(c.Failures, "; ")))
}

// cellText keeps free text inside one table cell: a pipe would end the cell,
// a line break the row, and a scrubbed <room> would read as an HTML tag and
// vanish.
func cellText(s string) string {
	return strings.NewReplacer("|", `\|`, "\r\n", " ", "\n", " ", "\r", " ", "<", "&lt;").Replace(s)
}

// keyMetrics are the numbers a person reads first, in a fixed order, each in
// its unit and only when the cell measured it.
func keyMetrics(c gate.Cell) string {
	const mbit, mib = 1e-6, 1.0 / (1 << 20)
	figures := []struct {
		key, format string
		scale       float64
	}{
		{gate.MetricThroughputDownBps, "↓ %.1f Mbit/s", mbit},
		{gate.MetricThroughputUpBps, "↑ %.1f Mbit/s", mbit},
		{gate.MetricConnectP95Ms, "connect p95 %.0f ms", 1},
		{gate.MetricOnTopP95Ms, "on-top p95 %.0f ms", 1},
		{gate.MetricHandshakeMs, "handshake %.0f ms", 1},
		{gate.MetricHeapPeakBytes, "heap peak %.1f MiB", mib},
		{gate.MetricRSSPeakBytes, "RSS peak %.1f MiB", mib},
		{gate.MetricMissedPong, "missed pongs %.0f", 1},
		{gate.MetricAnswered1, "burst 1 answered %.0f/64", 1},
		{gate.MetricAnswered2, "burst 2 answered %.0f/64", 1},
		{gate.MetricReady3sMs, "ready %.0f ms (bridge 3 s late)", 1},
		{gate.MetricReady8sMs, "ready %.0f ms (bridge 8 s late)", 1},
	}
	parts := make([]string, 0, len(figures))
	for _, f := range figures {
		if v, ok := c.Metrics[f.key]; ok {
			parts = append(parts, fmt.Sprintf(f.format, v*f.scale))
		}
	}
	return strings.Join(parts, ", ")
}

// short is a commit as a release quotes it: its first 12 characters.
func short(commit string) string {
	if len(commit) > 12 {
		return commit[:12]
	}
	return commit
}
