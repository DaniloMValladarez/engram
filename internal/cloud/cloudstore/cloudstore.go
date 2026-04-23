package cloudstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Gentleman-Programming/engram/internal/cloud"
	"github.com/Gentleman-Programming/engram/internal/cloud/chunkcodec"
	engramsync "github.com/Gentleman-Programming/engram/internal/sync"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type CloudStore struct {
	db *sql.DB
}

var ErrChunkNotFound = errors.New("cloudstore: chunk not found")
var ErrChunkConflict = errors.New("cloudstore: chunk id conflict")

func New(cfg cloud.Config) (*CloudStore, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, fmt.Errorf("cloudstore: database dsn is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("cloudstore: open postgres: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("cloudstore: ping postgres: %w", err)
	}
	store := &CloudStore{db: db}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (cs *CloudStore) Close() error {
	if cs == nil || cs.db == nil {
		return nil
	}
	return cs.db.Close()
}

type User struct {
	ID           string
	Username     string
	Email        string
	PasswordHash string
}

func (cs *CloudStore) CreateUser(username, email, _ string) (*User, error) {
	if cs == nil || cs.db == nil {
		return nil, fmt.Errorf("cloudstore: not initialized")
	}
	const q = `
		INSERT INTO cloud_users (username, email, password_hash)
		VALUES ($1, $2, '')
		ON CONFLICT (username) DO UPDATE SET email = EXCLUDED.email
		RETURNING id::text, username, email, password_hash`
	var u User
	if err := cs.db.QueryRowContext(context.Background(), q, strings.TrimSpace(username), strings.TrimSpace(email)).Scan(&u.ID, &u.Username, &u.Email, &u.PasswordHash); err != nil {
		return nil, fmt.Errorf("cloudstore: create user: %w", err)
	}
	return &u, nil
}

func (cs *CloudStore) GetUserByUsername(username string) (*User, error) {
	if cs == nil || cs.db == nil {
		return nil, fmt.Errorf("cloudstore: not initialized")
	}
	const q = `SELECT id::text, username, email, password_hash FROM cloud_users WHERE username = $1`
	var u User
	err := cs.db.QueryRowContext(context.Background(), q, strings.TrimSpace(username)).Scan(&u.ID, &u.Username, &u.Email, &u.PasswordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cloudstore: lookup user by username: %w", err)
	}
	return &u, nil
}

func (cs *CloudStore) GetUserByEmail(email string) (*User, error) {
	if cs == nil || cs.db == nil {
		return nil, fmt.Errorf("cloudstore: not initialized")
	}
	const q = `SELECT id::text, username, email, password_hash FROM cloud_users WHERE email = $1`
	var u User
	err := cs.db.QueryRowContext(context.Background(), q, strings.TrimSpace(email)).Scan(&u.ID, &u.Username, &u.Email, &u.PasswordHash)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cloudstore: lookup user by email: %w", err)
	}
	return &u, nil
}

func (cs *CloudStore) ReadManifest(ctx context.Context, project string) (*engramsync.Manifest, error) {
	if cs == nil || cs.db == nil {
		return nil, fmt.Errorf("cloudstore: not initialized")
	}
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, fmt.Errorf("cloudstore: project is required")
	}
	rows, err := cs.db.QueryContext(ctx, `
		SELECT chunk_id, created_by, COALESCE(client_created_at, created_at) AS manifest_created_at, sessions_count, observations_count, prompts_count, created_at
		FROM cloud_chunks
		WHERE project_name = $1
		ORDER BY created_at ASC, chunk_id ASC`, project)
	if err != nil {
		return nil, fmt.Errorf("cloudstore: query manifest: %w", err)
	}
	defer rows.Close()

	manifestRows := make([]manifestRow, 0)
	for rows.Next() {
		var row manifestRow
		if err := rows.Scan(&row.chunkID, &row.createdBy, &row.manifestTime, &row.sessions, &row.observations, &row.prompts, &row.serverCreated); err != nil {
			return nil, fmt.Errorf("cloudstore: scan manifest: %w", err)
		}
		manifestRows = append(manifestRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cloudstore: iterate manifest: %w", err)
	}
	return &engramsync.Manifest{Version: 1, Chunks: toManifestEntries(manifestRows)}, nil
}

type manifestRow struct {
	chunkID       string
	createdBy     string
	manifestTime  time.Time
	sessions      int
	observations  int
	prompts       int
	serverCreated time.Time
}

func toManifestEntries(rows []manifestRow) []engramsync.ChunkEntry {
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		if !left.serverCreated.Equal(right.serverCreated) {
			return left.serverCreated.Before(right.serverCreated)
		}
		return left.chunkID < right.chunkID
	})
	entries := make([]engramsync.ChunkEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, engramsync.ChunkEntry{
			ID:        row.chunkID,
			CreatedBy: row.createdBy,
			CreatedAt: row.manifestTime.UTC().Format(time.RFC3339),
			Sessions:  row.sessions,
			Memories:  row.observations,
			Prompts:   row.prompts,
		})
	}
	return entries
}

func (cs *CloudStore) WriteChunk(ctx context.Context, project, chunkID, createdBy, clientCreatedAt string, payload []byte) error {
	if cs == nil || cs.db == nil {
		return fmt.Errorf("cloudstore: not initialized")
	}
	project = strings.TrimSpace(project)
	if project == "" {
		return fmt.Errorf("cloudstore: project is required")
	}
	if strings.TrimSpace(chunkID) == "" {
		return fmt.Errorf("cloudstore: chunk id is required")
	}
	expectedChunkID := chunkIDFromPayload(payload)
	if chunkID != expectedChunkID {
		return fmt.Errorf("cloudstore: chunk id does not match payload hash (expected %s)", expectedChunkID)
	}
	originCreatedAt, err := parseClientCreatedAt(clientCreatedAt)
	if err != nil {
		return err
	}

	var existingPayload []byte
	err = cs.db.QueryRowContext(ctx, `SELECT payload::text FROM cloud_chunks WHERE project_name = $1 AND chunk_id = $2`, project, chunkID).Scan(&existingPayload)
	if err == nil {
		normalizedIncoming := normalizeJSON(payload)
		normalizedExisting := normalizeJSON(existingPayload)
		if string(normalizedIncoming) != string(normalizedExisting) {
			return fmt.Errorf("%w: existing chunk %q has different payload", ErrChunkConflict, chunkID)
		}
		_ = cs.indexChunkSessions(ctx, project, payload)
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("cloudstore: read existing chunk: %w", err)
	}

	counts := summarizeChunk(payload)
	_, err = cs.db.ExecContext(ctx, `
		INSERT INTO cloud_chunks (project_name, chunk_id, created_by, client_created_at, payload, sessions_count, observations_count, prompts_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		project, strings.TrimSpace(chunkID), strings.TrimSpace(createdBy), originCreatedAt, payload, counts.sessions, counts.observations, counts.prompts)
	if err != nil {
		if isUniqueViolation(err) {
			conflictErr := cs.resolveChunkConflict(ctx, project, chunkID, payload)
			if conflictErr != nil {
				return conflictErr
			}
			return nil
		}
		return fmt.Errorf("cloudstore: write chunk: %w", err)
	}
	if err := cs.indexChunkSessions(ctx, project, payload); err != nil {
		return err
	}
	return nil
}

func (cs *CloudStore) KnownSessionIDs(ctx context.Context, project string) (map[string]struct{}, error) {
	if cs == nil || cs.db == nil {
		return nil, fmt.Errorf("cloudstore: not initialized")
	}
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, fmt.Errorf("cloudstore: project is required")
	}
	rows, err := cs.db.QueryContext(ctx, `SELECT session_id FROM cloud_project_sessions WHERE project_name = $1`, project)
	if err != nil {
		return nil, fmt.Errorf("cloudstore: query session index: %w", err)
	}
	defer rows.Close()

	known := make(map[string]struct{})
	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			return nil, fmt.Errorf("cloudstore: scan session index: %w", err)
		}
		sessionID = strings.TrimSpace(sessionID)
		if sessionID == "" {
			continue
		}
		known[sessionID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cloudstore: iterate session index: %w", err)
	}
	return known, nil
}

func (cs *CloudStore) indexChunkSessions(ctx context.Context, project string, payload []byte) error {
	sessionIDs := collectSessionIDsFromPayload(payload)
	if len(sessionIDs) == 0 {
		return nil
	}
	for sessionID := range sessionIDs {
		if _, err := cs.db.ExecContext(ctx,
			`INSERT INTO cloud_project_sessions (project_name, session_id) VALUES ($1, $2) ON CONFLICT (project_name, session_id) DO NOTHING`,
			project, sessionID,
		); err != nil {
			return fmt.Errorf("cloudstore: index session %q: %w", sessionID, err)
		}
	}
	return nil
}

func (cs *CloudStore) backfillProjectSessionsFromChunks(ctx context.Context) error {
	rows, err := cs.db.QueryContext(ctx, `SELECT project_name, payload FROM cloud_chunks`)
	if err != nil {
		return fmt.Errorf("cloudstore: backfill session index: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var project string
		var payload []byte
		if err := rows.Scan(&project, &payload); err != nil {
			return fmt.Errorf("cloudstore: backfill session index scan: %w", err)
		}
		if err := cs.indexChunkSessions(ctx, project, payload); err != nil {
			return fmt.Errorf("cloudstore: backfill session index row: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("cloudstore: backfill session index iterate: %w", err)
	}
	return nil
}

func collectSessionIDsFromPayload(payload []byte) map[string]struct{} {
	chunk, err := parseChunkData(payload)
	if err != nil {
		return map[string]struct{}{}
	}
	return collectSessionIDs(chunk)
}

func parseChunkData(payload []byte) (engramsync.ChunkData, error) {
	var chunk engramsync.ChunkData
	if err := json.Unmarshal(payload, &chunk); err != nil {
		return engramsync.ChunkData{}, err
	}
	return chunk, nil
}

func collectSessionIDs(chunk engramsync.ChunkData) map[string]struct{} {
	sessionIDs := make(map[string]struct{})
	for _, session := range chunk.Sessions {
		sessionID := strings.TrimSpace(session.ID)
		if sessionID != "" {
			sessionIDs[sessionID] = struct{}{}
		}
	}
	for _, mutation := range chunk.Mutations {
		if mutation.Entity != "session" || mutation.Op == "delete" {
			continue
		}
		mutationPayload := strings.TrimSpace(mutation.Payload)
		if mutationPayload == "" {
			continue
		}
		var body struct {
			ID string `json:"id"`
		}
		if err := chunkcodec.DecodeSyncMutationPayload(mutationPayload, &body); err != nil {
			continue
		}
		sessionID := strings.TrimSpace(body.ID)
		if sessionID != "" {
			sessionIDs[sessionID] = struct{}{}
		}
	}
	return sessionIDs
}

func (cs *CloudStore) resolveChunkConflict(ctx context.Context, project, chunkID string, payload []byte) error {
	var existingPayload []byte
	err := cs.db.QueryRowContext(ctx, `SELECT payload::text FROM cloud_chunks WHERE project_name = $1 AND chunk_id = $2`, project, chunkID).Scan(&existingPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: existing chunk %q was concurrently inserted", ErrChunkConflict, chunkID)
	}
	if err != nil {
		return fmt.Errorf("cloudstore: resolve chunk conflict: %w", err)
	}
	normalizedIncoming := normalizeJSON(payload)
	normalizedExisting := normalizeJSON(existingPayload)
	if string(normalizedIncoming) == string(normalizedExisting) {
		return nil
	}
	return fmt.Errorf("%w: existing chunk %q has different payload", ErrChunkConflict, chunkID)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "23505"
}

func (cs *CloudStore) ReadChunk(ctx context.Context, project, chunkID string) ([]byte, error) {
	if cs == nil || cs.db == nil {
		return nil, fmt.Errorf("cloudstore: not initialized")
	}
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, fmt.Errorf("cloudstore: project is required")
	}
	var payload []byte
	err := cs.db.QueryRowContext(ctx, `SELECT payload FROM cloud_chunks WHERE project_name = $1 AND chunk_id = $2`, project, strings.TrimSpace(chunkID)).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%w: %q", ErrChunkNotFound, chunkID)
	}
	if err != nil {
		return nil, fmt.Errorf("cloudstore: read chunk: %w", err)
	}
	return payload, nil
}

func (cs *CloudStore) migrate(ctx context.Context) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS cloud_users (
			id BIGSERIAL PRIMARY KEY,
			username TEXT UNIQUE NOT NULL,
			email TEXT UNIQUE NOT NULL,
			password_hash TEXT NOT NULL DEFAULT '',
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS cloud_chunks (
			project_name TEXT NOT NULL DEFAULT 'default',
			chunk_id TEXT NOT NULL,
			created_by TEXT NOT NULL,
			client_created_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			payload JSONB NOT NULL,
			sessions_count INTEGER NOT NULL DEFAULT 0,
			observations_count INTEGER NOT NULL DEFAULT 0,
			prompts_count INTEGER NOT NULL DEFAULT 0
		)`,
		`ALTER TABLE cloud_chunks ADD COLUMN IF NOT EXISTS project_name TEXT`,
		`ALTER TABLE cloud_chunks ADD COLUMN IF NOT EXISTS client_created_at TIMESTAMPTZ`,
		`UPDATE cloud_chunks SET project_name = 'default' WHERE project_name IS NULL OR btrim(project_name) = ''`,
		`ALTER TABLE cloud_chunks ALTER COLUMN project_name SET NOT NULL`,
		`DO $$ BEGIN
			IF EXISTS (
				SELECT 1 FROM pg_constraint
				WHERE conname = 'cloud_chunks_pkey' AND conrelid = 'cloud_chunks'::regclass
			) THEN
				ALTER TABLE cloud_chunks DROP CONSTRAINT cloud_chunks_pkey;
			END IF;
		END $$`,
		`CREATE UNIQUE INDEX IF NOT EXISTS cloud_chunks_project_chunk_uidx ON cloud_chunks (project_name, chunk_id)`,
		`CREATE TABLE IF NOT EXISTS cloud_project_sessions (
			project_name TEXT NOT NULL,
			session_id TEXT NOT NULL,
			indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (project_name, session_id)
		)`,
		`INSERT INTO cloud_project_sessions (project_name, session_id)
		 SELECT c.project_name, btrim(elem->>'id')
		 FROM cloud_chunks c,
		      jsonb_array_elements(COALESCE(c.payload->'sessions', '[]'::jsonb)) AS elem
		 WHERE btrim(COALESCE(elem->>'id', '')) <> ''
		 ON CONFLICT (project_name, session_id) DO NOTHING`,
	}
	for _, q := range queries {
		if _, err := cs.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("cloudstore: migrate: %w", err)
		}
	}
	if err := cs.backfillProjectSessionsFromChunks(ctx); err != nil {
		return err
	}
	return nil
}

func parseClientCreatedAt(value string) (*time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return nil, fmt.Errorf("cloudstore: invalid client_created_at: %w", err)
	}
	parsed = parsed.UTC()
	return &parsed, nil
}

func chunkIDFromPayload(payload []byte) string {
	return chunkcodec.ChunkID(payload)
}

func normalizeJSON(payload []byte) []byte {
	var body any
	if err := json.Unmarshal(payload, &body); err != nil {
		return payload
	}
	normalized, err := json.Marshal(body)
	if err != nil {
		return payload
	}
	return normalized
}

type chunkSummary struct {
	sessions     int
	observations int
	prompts      int
}

func summarizeChunk(payload []byte) chunkSummary {
	var body struct {
		Sessions     []json.RawMessage `json:"sessions"`
		Observations []json.RawMessage `json:"observations"`
		Prompts      []json.RawMessage `json:"prompts"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return chunkSummary{}
	}
	return chunkSummary{
		sessions:     len(body.Sessions),
		observations: len(body.Observations),
		prompts:      len(body.Prompts),
	}
}
