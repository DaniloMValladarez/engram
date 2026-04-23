package cloudstore

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Gentleman-Programming/engram/internal/cloud"
	engramsync "github.com/Gentleman-Programming/engram/internal/sync"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestNewRequiresDSN(t *testing.T) {
	_, err := New(cloud.Config{})
	if err == nil {
		t.Fatal("expected error when DSN is empty")
	}
}

func TestSummarizeChunkCountsEntities(t *testing.T) {
	counts := summarizeChunk([]byte(`{"sessions":[{"id":"s1"}],"observations":[{"id":1},{"id":2}],"prompts":[{"id":3}]}`))
	if counts.sessions != 1 || counts.observations != 2 || counts.prompts != 1 {
		t.Fatalf("unexpected counts: %+v", counts)
	}

	empty := summarizeChunk([]byte(`{`))
	if empty.sessions != 0 || empty.observations != 0 || empty.prompts != 0 {
		t.Fatalf("invalid json must return zero counts, got %+v", empty)
	}
}

func TestChunkIDFromPayloadStable(t *testing.T) {
	payload := []byte(`{"sessions":[{"id":"s1"}],"observations":[],"prompts":[]}`)
	if got := chunkIDFromPayload(payload); got == "" || len(got) != 8 {
		t.Fatalf("expected 8-char chunk id, got %q", got)
	}
}

func TestNormalizeJSONCanonicalizesEquivalentPayloads(t *testing.T) {
	a := []byte(`{"a":1,"b":[2,3]}`)
	b := []byte("{\n  \"b\": [2,3], \"a\":1\n}")
	if string(normalizeJSON(a)) != string(normalizeJSON(b)) {
		t.Fatalf("expected normalized payloads to match")
	}
}

func TestErrorSentinels(t *testing.T) {
	if !errors.Is(ErrChunkNotFound, ErrChunkNotFound) {
		t.Fatalf("expected ErrChunkNotFound to be comparable")
	}
	if !errors.Is(ErrChunkConflict, ErrChunkConflict) {
		t.Fatalf("expected ErrChunkConflict to be comparable")
	}
}

func TestIsUniqueViolation(t *testing.T) {
	if !isUniqueViolation(&pgconn.PgError{Code: "23505"}) {
		t.Fatal("expected Postgres unique violation to be detected")
	}
	if isUniqueViolation(errors.New("boom")) {
		t.Fatal("expected non-pg error to return false")
	}
}

func TestCollectSessionIDsIncludesChunkSessionsAndMutationSessions(t *testing.T) {
	sessionIDs := collectSessionIDsFromPayload([]byte(`{
		"sessions":[{"id":"s-1"}],
		"mutations":[
			{"entity":"session","op":"upsert","payload":"{\"id\":\"s-2\",\"directory\":\"/tmp/s-2\"}"},
			{"entity":"session","op":"upsert","payload":"\"{\\\"id\\\":\\\"s-3\\\",\\\"directory\\\":\\\"/tmp/s-3\\\"}\""},
			{"entity":"observation","op":"upsert","payload":"{\"session_id\":\"s-1\"}"}
		]
	}`))

	if _, ok := sessionIDs["s-1"]; !ok {
		t.Fatalf("expected session id from chunk sessions")
	}
	if _, ok := sessionIDs["s-2"]; !ok {
		t.Fatalf("expected session id from mutation payload")
	}
	if _, ok := sessionIDs["s-3"]; !ok {
		t.Fatalf("expected session id from double-encoded mutation payload")
	}
}

func TestParseClientCreatedAt(t *testing.T) {
	t.Run("empty is allowed", func(t *testing.T) {
		got, err := parseClientCreatedAt("")
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if got != nil {
			t.Fatalf("expected nil timestamp for empty input, got %v", got)
		}
	})

	t.Run("valid RFC3339", func(t *testing.T) {
		got, err := parseClientCreatedAt("2026-04-01T12:30:00Z")
		if err != nil {
			t.Fatalf("expected valid parse, got %v", err)
		}
		if got == nil || got.Format(time.RFC3339) != "2026-04-01T12:30:00Z" {
			t.Fatalf("unexpected timestamp parse result: %v", got)
		}
	})

	t.Run("invalid format returns error", func(t *testing.T) {
		if _, err := parseClientCreatedAt("not-a-time"); err == nil {
			t.Fatal("expected parse error for invalid timestamp")
		}
	})
}

func TestSortManifestRowsByServerCreatedAtForReplay(t *testing.T) {
	rows := []manifestRow{
		{
			chunkID:       "chunk-newer-client-time",
			createdBy:     "dev-a",
			manifestTime:  time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC),
			serverCreated: time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC),
		},
		{
			chunkID:       "chunk-older-client-time",
			createdBy:     "dev-b",
			manifestTime:  time.Date(2026, 4, 9, 10, 0, 0, 0, time.UTC),
			serverCreated: time.Date(2026, 4, 10, 9, 0, 0, 0, time.UTC),
		},
	}

	entries := toManifestEntries(rows)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	gotOrder := []string{entries[0].ID, entries[1].ID}
	wantOrder := []string{"chunk-older-client-time", "chunk-newer-client-time"}
	if !slices.Equal(gotOrder, wantOrder) {
		t.Fatalf("expected server-created ordering %v, got %v", wantOrder, gotOrder)
	}

	if entries[0].CreatedAt != "2026-04-09T10:00:00Z" || entries[1].CreatedAt != "2026-04-08T10:00:00Z" {
		t.Fatalf("expected manifest created_at to preserve metadata timestamps, got %+v", entries)
	}
}

func TestSortManifestRowsBreaksServerTimeTiesByChunkID(t *testing.T) {
	serverTimestamp := time.Date(2026, 4, 10, 10, 0, 0, 0, time.UTC)
	rows := []manifestRow{
		{chunkID: "b", createdBy: "dev", manifestTime: serverTimestamp, serverCreated: serverTimestamp},
		{chunkID: "a", createdBy: "dev", manifestTime: serverTimestamp, serverCreated: serverTimestamp},
	}
	entries := toManifestEntries(rows)
	gotOrder := []string{entries[0].ID, entries[1].ID}
	wantOrder := []string{"a", "b"}
	if !slices.Equal(gotOrder, wantOrder) {
		t.Fatalf("expected deterministic chunk-id tie-break ordering %v, got %v", wantOrder, gotOrder)
	}
}

func parseMustChunk(t *testing.T, payload []byte) engramsync.ChunkData {
	t.Helper()
	chunk, err := parseChunkData(payload)
	if err != nil {
		t.Fatalf("parse chunk data: %v", err)
	}
	return chunk
}
