package salutejazz

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openlibrecommunity/olcrtc/internal/auth"
)

func TestIssueRunsPreconnectAndReturnsConnectorCredentials(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/room/abc123/preconnect" {
			t.Errorf("path %q", r.URL.Path)
		}
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["password"] != "passw0rd" {
			t.Errorf("password %q", body["password"])
		}
		if r.Header.Get("Origin") != "https://salutejazz.ru" {
			t.Errorf("origin %q", r.Header.Get("Origin"))
		}
		_, _ = w.Write([]byte(`{"connectorUrl":"wss://fake/connector","roomTitle":"t","participantRole":"MEMBER"}`))
	}))
	defer srv.Close()

	p := Provider{apiBase: srv.URL}
	creds, err := p.Issue(context.Background(), auth.Config{RoomURL: "abc123:passw0rd", Name: "n"})
	if err != nil {
		t.Fatal(err)
	}
	if creds.URL != "wss://fake/connector" || creds.Token != "passw0rd" || creds.Extra["roomID"] != "abc123" {
		t.Fatalf("creds %+v", creds)
	}
}

func TestIssueRejectsMalformedRoomReferences(t *testing.T) {
	p := Provider{}
	for _, bad := range []string{"", "any", "abc123", "abc123:", ":passw0rd", "abc:pass:extra", "ABC123:passw0rd"} {
		if _, err := p.Issue(context.Background(), auth.Config{RoomURL: bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestCreateRoomReturnsCodeAndPassword(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/room/create-meeting" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"roomId":"zzz999","password":"pw123456","url":"https://salutejazz.ru/zzz999?psw=x"}`))
	}))
	defer srv.Close()

	p := Provider{apiBase: srv.URL}
	room, err := p.CreateRoom(context.Background(), auth.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if room != "zzz999:pw123456" {
		t.Fatalf("room %q", room)
	}
}
