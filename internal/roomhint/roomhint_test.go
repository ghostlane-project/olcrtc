package roomhint

import "testing"

func TestSplit(t *testing.T) {
	const room = "https://my.mts-link.ru/j/123/456"
	for _, tc := range []struct{ in, room, mode string }{
		{room, room, ICEDefault},
		{room + "?ice=tcp", room, ICETCP},
		{room + "?ice=both", room, ICEBoth},
		{room + "?ice=udp", room, ICEUDP},
		{" " + room + "?ice=tcp ", room, ICETCP},
		{room + "?x=1&ice=tcp", room + "?x=1", ICETCP},
		// unknown modes and lookalikes leave the link as it is
		{room + "?ice=quic", room + "?ice=quic", ICEDefault},
		{room + "?ice=tcp&x=1", room + "?ice=tcp&x=1", ICEDefault},
		{"https://telemost.yandex.ru/j/123", "https://telemost.yandex.ru/j/123", ICEDefault},
	} {
		gotRoom, gotMode := Split(tc.in)
		if gotRoom != tc.room || gotMode != tc.mode {
			t.Errorf("Split(%q) = %q, %q; want %q, %q", tc.in, gotRoom, gotMode, tc.room, tc.mode)
		}
	}
}
