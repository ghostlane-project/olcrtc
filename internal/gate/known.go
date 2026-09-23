package gate

import (
	"errors"
	"slices"
	"strings"
)

// ai-generated: the whole file (the known failures: cells an open engine
// issue fails on every run; they still run and are reported with their
// issue, but their failures fail neither TestGate nor the app's verdict).

// ErrKnownFailure is what a known failure's run returns next to
// ErrCellFailed: the entry logs the cell instead of failing its subtest.
var ErrKnownFailure = errors.New("known failure")

// issues is where the engine's issues are. Kept when the list is empty: the
// next entry writes issues + "<number>", and a constant that comes back with
// every entry is not one to delete with the last one.
const issues = "https://github.com/ghostlane-project/olcrtc/issues/"

// KnownFailure is a cell that fails every run until the issue that tracks it
// is fixed. Cell is a cell id pattern in which "*" is exactly one whole
// segment and any other segment stands for itself; Issue is the open issue's
// URL; Why says what fails, in a few words.
type KnownFailure struct{ Cell, Issue, Why string }

// knownFailures is the list, one entry per line. Every entry needs an open
// issue, and goes once its issue is closed and its cells pass. A wbstream
// cell may be on it for an engine bug: without the WB token, or with a room
// that will not open, the server never comes up, its cells never run, and a
// cell that never ran is never known, so configuration stays blocking.
//
// ai-generated: the salutejazz entry, narrowed to S2. With the relay window
// bounding what Sber's SFU queues toward a peer, a salutejazz session that
// ends on liveness is a regression again, so S0, S1, S3, S4, S5 and S7
// block; S3, upload saturation, passes since the window (the engine gate
// on 7b78fd4a and the Ghostlane 1.0.440 release gate). S2, download
// saturation, does not on a slow Sber leg: at 26-62 kB/s delivered and a
// 3-7 s echo round trip (the engine gate on a GitHub runner) its pulls
// still end on a liveness close, and a connect on top waits behind up to a
// window of data. S2 stays on #49.
var knownFailures = []KnownFailure{ //nolint:gochecknoglobals // edited by hand as issues open and close; tests swap it
	{Cell: "engine-linux/salutejazz/datachannel/*/S2", Issue: issues + "49",
		Why: "liveness close, throughput and on-top latency on a slow Sber leg"},
}

// knownFailure is the entry of the list a cell id matches, the first one if
// several do.
func knownFailure(cellID string) (KnownFailure, bool) {
	for _, k := range knownFailures {
		if cellMatches(k.Cell, cellID) {
			return k, true
		}
	}
	return KnownFailure{}, false
}

// cellMatches says whether a cell id fits a pattern: as many segments, each
// one the pattern's own or under a "*" there. A "*" is a whole segment: it
// matches neither part of one nor several, nor an empty one.
func cellMatches(pattern, cellID string) bool {
	return slices.EqualFunc(strings.Split(pattern, "/"), strings.Split(cellID, "/"), func(p, s string) bool {
		return s != "" && (p == "*" || p == s)
	})
}
