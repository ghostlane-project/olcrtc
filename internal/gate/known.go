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

// issues is where the engine's issues are. Kept with the list empty: the
// next entry writes issues + "<number>", and a constant that comes back with
// every entry is not one to delete with the last one.
//
//nolint:unused // the list is empty today; every entry uses this.
const issues = "https://github.com/romanpodpriatov/olcrtc/issues/"

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
var knownFailures = []KnownFailure{ //nolint:gochecknoglobals // edited by hand as issues open and close; tests swap it
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
