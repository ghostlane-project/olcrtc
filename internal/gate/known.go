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

// issues is where the engine's issues are.
const issues = "https://github.com/romanpodpriatov/olcrtc/issues/"

// KnownFailure is a cell that fails every run until the issue that tracks it
// is fixed. Cell is a cell id pattern in which "*" is exactly one whole
// segment and any other segment stands for itself; Issue is the open issue's
// URL; Why says what fails, in a few words.
type KnownFailure struct{ Cell, Issue, Why string }

// knownFailures is the list, one entry per line. Every entry needs an open
// issue, and goes once its issue is closed and its cells pass. A wbstream
// cell is never on it: without the WB token those cells fail on
// configuration, which must stay a blocking failure.
var knownFailures = []KnownFailure{ //nolint:gochecknoglobals // edited by hand as issues open and close; tests swap it
	{"engine-linux/jitsi/seichannel/*/S0", issues + "9", "a 10 MiB pull through Jitsi kills the tunnel"},
	{"engine-linux/jitsi/datachannel/*/S6", issues + "10", "a bridge 8 s late overruns the 15 s handshake window"},
	{"engine-linux/jitsi/datachannel/mobile/S5", issues + "11", "a DNS burst holds some first bytes 2 s"},
	{"engine-linux/jitsi/vp8channel/mobile/S0", issues + "12", "a pull stalls at 4.5 MiB while control lives"},
	{"engine-linux/jitsi/datachannel/mobile/S2", issues + "15", "six parallel pulls collapse into SCTP retransmissions"},
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
