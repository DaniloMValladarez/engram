package cloudserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Gentleman-Programming/engram/internal/cloud/chunkcodec"
	"github.com/Gentleman-Programming/engram/internal/cloud/cloudstore"
	"github.com/Gentleman-Programming/engram/internal/cloud/constants"
	"github.com/Gentleman-Programming/engram/internal/cloud/dashboard"
	"github.com/Gentleman-Programming/engram/internal/store"
	engramsync "github.com/Gentleman-Programming/engram/internal/sync"
)

type Option func(*CloudServer)

type ChunkStore interface {
	ReadManifest(ctx context.Context, project string) (*engramsync.Manifest, error)
	WriteChunk(ctx context.Context, project, chunkID, createdBy, clientCreatedAt string, payload []byte) error
	ReadChunk(ctx context.Context, project, chunkID string) ([]byte, error)
	KnownSessionIDs(ctx context.Context, project string) (map[string]struct{}, error)
}

type Authenticator interface {
	Authorize(r *http.Request) error
}

type ProjectAuthorizer interface {
	AuthorizeProject(project string) error
}

type dashboardSessionCodec interface {
	MintDashboardSession(bearerToken string) (string, error)
	ParseDashboardSession(sessionToken string) (string, error)
}

type staticStatusProvider struct{ status dashboard.SyncStatus }

func (s staticStatusProvider) Status() dashboard.SyncStatus { return s.status }

type CloudServer struct {
	store          ChunkStore
	auth           Authenticator
	projectAuth    ProjectAuthorizer
	port           int
	host           string
	mux            *http.ServeMux
	syncStatus     dashboard.SyncStatusProvider
	listenAndServe func(addr string, handler http.Handler) error
}

const defaultHost = "127.0.0.1"
const maxPushBodyBytes int64 = 8 * 1024 * 1024
const maxDashboardLoginBodyBytes int64 = 16 * 1024
const dashboardSessionCookieName = "engram_dashboard_token"

var ErrDashboardSessionCodecRequired = errors.New("dashboard session codec is required for dashboard auth")

func WithSyncStatusProvider(provider dashboard.SyncStatusProvider) Option {
	return func(s *CloudServer) {
		s.syncStatus = provider
	}
}

func WithHost(host string) Option {
	return func(s *CloudServer) {
		s.host = strings.TrimSpace(host)
	}
}

func WithProjectAuthorizer(authorizer ProjectAuthorizer) Option {
	return func(s *CloudServer) {
		s.projectAuth = authorizer
	}
}

func New(store ChunkStore, authSvc Authenticator, port int, opts ...Option) *CloudServer {
	s := &CloudServer{
		store: store,
		auth:  authSvc,
		port:  port,
		host:  defaultHost,
		syncStatus: staticStatusProvider{status: dashboard.SyncStatus{
			Phase:         "degraded",
			ReasonCode:    constants.ReasonTransportFailed,
			ReasonMessage: "sync status provider is unavailable",
		}},
		listenAndServe: http.ListenAndServe,
	}
	if projectAuthorizer, ok := authSvc.(ProjectAuthorizer); ok {
		s.projectAuth = projectAuthorizer
	}
	for _, opt := range opts {
		opt(s)
	}
	s.routes()
	return s
}

func (s *CloudServer) Start() error {
	host := strings.TrimSpace(s.host)
	if host == "" {
		host = defaultHost
	}
	addr := fmt.Sprintf("%s:%d", host, s.port)
	log.Printf("[engram-cloud] listening on %s", addr)
	return s.listenAndServe(addr, s.Handler())
}

func (s *CloudServer) Handler() http.Handler {
	if s.mux == nil {
		s.routes()
	}
	return s.mux
}

func (s *CloudServer) routes() {
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("GET /health", s.handleHealth)
	dashboardHandler := dashboard.HandlerWithStatus(s.syncStatus)
	s.mux.HandleFunc("GET /dashboard/login", s.handleDashboardLoginPage)
	s.mux.HandleFunc("POST /dashboard/login", s.handleDashboardLogin)
	s.mux.HandleFunc("POST /dashboard/logout", s.handleDashboardLogout)
	s.mux.Handle("/dashboard", s.withDashboardAuthHandler(dashboardHandler))
	s.mux.Handle("/dashboard/", s.withDashboardAuthHandler(dashboardHandler))
	s.mux.HandleFunc("GET /sync/pull", s.withAuth(s.handlePullManifest))
	s.mux.HandleFunc("GET /sync/pull/{chunkID}", s.withAuth(s.handlePullChunk))
	s.mux.HandleFunc("POST /sync/push", s.withAuth(s.handlePushChunk))
}

func (s *CloudServer) withAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.auth != nil {
			if err := s.auth.Authorize(r); err != nil {
				http.Error(w, fmt.Sprintf("unauthorized: %v", err), http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func (s *CloudServer) withAuthHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.auth != nil {
			if err := s.auth.Authorize(r); err != nil {
				http.Error(w, fmt.Sprintf("unauthorized: %v", err), http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func (s *CloudServer) withDashboardAuthHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.auth == nil {
			next.ServeHTTP(w, r)
			return
		}
		if err := s.authorizeDashboardRequest(r); err != nil {
			http.Redirect(w, r, "/dashboard/login", http.StatusSeeOther)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *CloudServer) authorizeDashboardRequest(r *http.Request) error {
	if s.auth == nil {
		return nil
	}
	if err := s.auth.Authorize(r); err == nil {
		return nil
	}
	cookie, err := r.Cookie(dashboardSessionCookieName)
	if err != nil {
		return err
	}
	bearerToken, err := s.dashboardBearerToken(cookie.Value)
	if err != nil {
		return err
	}
	if strings.TrimSpace(bearerToken) == "" {
		return fmt.Errorf("dashboard session token is empty")
	}
	clone := r.Clone(r.Context())
	clone.Header = r.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+bearerToken)
	return s.auth.Authorize(clone)
}

func (s *CloudServer) handleDashboardLoginPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.auth == nil {
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
		return
	}
	if err := s.authorizeDashboardRequest(r); err == nil {
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
		return
	}
	renderDashboardLoginPage(w, "")
}

func (s *CloudServer) handleDashboardLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.auth == nil {
		http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxDashboardLoginBodyBytes)
	if err := r.ParseForm(); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, fmt.Sprintf("login payload too large (max %d bytes)", maxDashboardLoginBodyBytes), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "invalid form payload", http.StatusBadRequest)
		return
	}
	token := strings.TrimSpace(r.PostForm.Get("token"))
	if token == "" {
		renderDashboardLoginPage(w, "token is required")
		return
	}
	testReq := r.Clone(r.Context())
	testReq.Header = r.Header.Clone()
	testReq.Header.Set("Authorization", "Bearer "+token)
	if err := s.auth.Authorize(testReq); err != nil {
		renderDashboardLoginPage(w, "invalid token")
		return
	}
	sessionToken, err := s.dashboardSessionToken(token)
	if err != nil {
		http.Error(w, "unable to create dashboard session", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     dashboardSessionCookieName,
		Value:    sessionToken,
		Path:     "/dashboard",
		HttpOnly: true,
		Secure:   dashboardCookieSecure(r),
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int((8 * time.Hour).Seconds()),
	})
	http.Redirect(w, r, "/dashboard", http.StatusSeeOther)
}

func (s *CloudServer) handleDashboardLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name:     dashboardSessionCookieName,
		Value:    "",
		Path:     "/dashboard",
		HttpOnly: true,
		Secure:   dashboardCookieSecure(r),
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
	http.Redirect(w, r, "/dashboard/login", http.StatusSeeOther)
}

func (s *CloudServer) dashboardSessionToken(bearerToken string) (string, error) {
	if codec, ok := s.auth.(dashboardSessionCodec); ok {
		return codec.MintDashboardSession(bearerToken)
	}
	return "", ErrDashboardSessionCodecRequired
}

func (s *CloudServer) dashboardBearerToken(sessionToken string) (string, error) {
	sessionToken = strings.TrimSpace(sessionToken)
	if sessionToken == "" {
		return "", fmt.Errorf("dashboard session token is empty")
	}
	if codec, ok := s.auth.(dashboardSessionCodec); ok {
		return codec.ParseDashboardSession(sessionToken)
	}
	return "", ErrDashboardSessionCodecRequired
}

func dashboardCookieSecure(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	forwardedProto := r.Header.Get("X-Forwarded-Proto")
	for _, proto := range strings.Split(forwardedProto, ",") {
		if strings.EqualFold(strings.TrimSpace(proto), "https") {
			return true
		}
	}
	return false
}

func renderDashboardLoginPage(w http.ResponseWriter, errorMessage string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	message := ""
	if strings.TrimSpace(errorMessage) != "" {
		message = fmt.Sprintf("<p style=\"color:#b91c1c\">%s</p>", html.EscapeString(errorMessage))
	}
	_, _ = w.Write([]byte(fmt.Sprintf(`<!doctype html>
<html>
<head><title>Engram Cloud Dashboard Login</title></head>
<body>
  <main>
    <h1>Dashboard Login</h1>
    <p>Paste your cloud bearer token to start a dashboard session cookie.</p>
    %s
    <form method="post" action="/dashboard/login">
      <label for="token">Bearer token</label>
      <input id="token" name="token" type="password" autocomplete="off" required />
      <button type="submit">Sign in</button>
    </form>
  </main>
</body>
</html>`, message)))
}

func (s *CloudServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	jsonResponse(w, http.StatusOK, map[string]any{"status": "ok", "service": "engram-cloud"})
}

func (s *CloudServer) handlePullManifest(w http.ResponseWriter, r *http.Request) {
	project, ok := projectFromRequest(w, r)
	if !ok {
		return
	}
	if !s.authorizeProjectScope(w, project) {
		return
	}
	manifest, err := s.store.ReadManifest(r.Context(), project)
	if err != nil {
		http.Error(w, fmt.Sprintf("read manifest: %v", err), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, http.StatusOK, manifest)
}

func (s *CloudServer) handlePullChunk(w http.ResponseWriter, r *http.Request) {
	project, ok := projectFromRequest(w, r)
	if !ok {
		return
	}
	if !s.authorizeProjectScope(w, project) {
		return
	}
	chunkID := strings.TrimSpace(r.PathValue("chunkID"))
	if chunkID == "" {
		http.Error(w, "chunkID is required", http.StatusBadRequest)
		return
	}
	chunk, err := s.store.ReadChunk(r.Context(), project, chunkID)
	if err != nil {
		if errors.Is(err, cloudstore.ErrChunkNotFound) {
			http.Error(w, fmt.Sprintf("read chunk: %v", err), http.StatusNotFound)
			return
		}
		http.Error(w, fmt.Sprintf("read chunk: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(chunk)
}

func (s *CloudServer) handlePushChunk(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxPushBodyBytes)
	var req struct {
		ChunkID         string          `json:"chunk_id"`
		CreatedBy       string          `json:"created_by"`
		ClientCreatedAt string          `json:"client_created_at"`
		Project         string          `json:"project"`
		Data            json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, fmt.Sprintf("push payload too large (max %d bytes)", maxPushBodyBytes), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, fmt.Sprintf("invalid push payload: %v", err), http.StatusBadRequest)
		return
	}
	if len(req.Data) == 0 {
		http.Error(w, "data is required", http.StatusBadRequest)
		return
	}
	project := strings.TrimSpace(req.Project)
	if project == "" {
		project = strings.TrimSpace(r.URL.Query().Get("project"))
	}
	if project == "" {
		http.Error(w, "project is required", http.StatusBadRequest)
		return
	}
	project, _ = store.NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		http.Error(w, "project is required", http.StatusBadRequest)
		return
	}
	if !s.authorizeProjectScope(w, project) {
		return
	}

	normalizedData, err := coerceChunkProject(req.Data, project)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid push payload: %v", err), http.StatusBadRequest)
		return
	}
	chunk, err := validateImportableChunkPayload(normalizedData)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid push payload: %v", err), http.StatusBadRequest)
		return
	}
	knownSessionIDs, err := s.store.KnownSessionIDs(r.Context(), project)
	if err != nil {
		http.Error(w, fmt.Sprintf("validate push payload: %v", err), http.StatusInternalServerError)
		return
	}
	if err := validateChunkSessionReferences(chunk, knownSessionIDs); err != nil {
		http.Error(w, fmt.Sprintf("invalid push payload: %v", err), http.StatusBadRequest)
		return
	}

	computedChunkID := chunkIDFromPayload(normalizedData)
	providedChunkID := strings.TrimSpace(req.ChunkID)
	if providedChunkID != "" && providedChunkID != computedChunkID {
		http.Error(w, fmt.Sprintf("chunk_id does not match payload content hash (expected %s)", computedChunkID), http.StatusBadRequest)
		return
	}
	clientCreatedAt := strings.TrimSpace(req.ClientCreatedAt)
	if clientCreatedAt != "" {
		if _, err := time.Parse(time.RFC3339, clientCreatedAt); err != nil {
			http.Error(w, "client_created_at must be RFC3339", http.StatusBadRequest)
			return
		}
	}

	if err := s.store.WriteChunk(r.Context(), project, computedChunkID, req.CreatedBy, clientCreatedAt, normalizedData); err != nil {
		if errors.Is(err, cloudstore.ErrChunkConflict) {
			http.Error(w, fmt.Sprintf("write chunk: %v", err), http.StatusConflict)
			return
		}
		http.Error(w, fmt.Sprintf("write chunk: %v", err), http.StatusInternalServerError)
		return
	}
	jsonResponse(w, http.StatusOK, map[string]any{"status": "ok", "chunk_id": computedChunkID})
}

func chunkIDFromPayload(payload []byte) string {
	return chunkcodec.ChunkID(payload)
}

func projectFromRequest(w http.ResponseWriter, r *http.Request) (string, bool) {
	project := strings.TrimSpace(r.URL.Query().Get("project"))
	if project == "" {
		http.Error(w, "project is required", http.StatusBadRequest)
		return "", false
	}
	project, _ = store.NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		http.Error(w, "project is required", http.StatusBadRequest)
		return "", false
	}
	return project, true
}

func (s *CloudServer) authorizeProjectScope(w http.ResponseWriter, project string) bool {
	if s.projectAuth == nil {
		return true
	}
	if err := s.projectAuth.AuthorizeProject(project); err != nil {
		http.Error(w, "forbidden: project is not allowed", http.StatusForbidden)
		return false
	}
	return true
}

func coerceChunkProject(payload []byte, project string) ([]byte, error) {
	return chunkcodec.CanonicalizeForProject(payload, project)
}

func decodeSyncMutationPayload(payload string, dest any) error {
	return chunkcodec.DecodeSyncMutationPayload(payload, dest)
}

func validateImportableChunkPayload(payload []byte) (engramsync.ChunkData, error) {
	var chunk engramsync.ChunkData
	if err := json.Unmarshal(payload, &chunk); err != nil {
		return engramsync.ChunkData{}, fmt.Errorf("chunk schema: %w", err)
	}
	if err := validateDirectChunkArrayEntries(chunk); err != nil {
		return engramsync.ChunkData{}, err
	}
	return chunk, nil

}

func validateDirectChunkArrayEntries(chunk engramsync.ChunkData) error {
	for i, session := range chunk.Sessions {
		if strings.TrimSpace(session.ID) == "" {
			return fmt.Errorf("sessions[%d].id is required", i)
		}
		if strings.TrimSpace(session.Directory) == "" {
			return fmt.Errorf("sessions[%d].directory is required", i)
		}
	}

	for i, observation := range chunk.Observations {
		if strings.TrimSpace(observation.SyncID) == "" {
			return fmt.Errorf("observations[%d].sync_id is required", i)
		}
		if strings.TrimSpace(observation.SessionID) == "" {
			return fmt.Errorf("observations[%d].session_id is required", i)
		}
		if strings.TrimSpace(observation.Type) == "" {
			return fmt.Errorf("observations[%d].type is required", i)
		}
		if strings.TrimSpace(observation.Title) == "" {
			return fmt.Errorf("observations[%d].title is required", i)
		}
		if strings.TrimSpace(observation.Content) == "" {
			return fmt.Errorf("observations[%d].content is required", i)
		}
		if strings.TrimSpace(observation.Scope) == "" {
			return fmt.Errorf("observations[%d].scope is required", i)
		}
	}

	for i, prompt := range chunk.Prompts {
		if strings.TrimSpace(prompt.SyncID) == "" {
			return fmt.Errorf("prompts[%d].sync_id is required", i)
		}
		if strings.TrimSpace(prompt.SessionID) == "" {
			return fmt.Errorf("prompts[%d].session_id is required", i)
		}
		if strings.TrimSpace(prompt.Content) == "" {
			return fmt.Errorf("prompts[%d].content is required", i)
		}
	}

	return nil
}

func validateChunkSessionReferences(chunk engramsync.ChunkData, knownSessionIDs map[string]struct{}) error {
	chunkSessionIDs := make(map[string]struct{}, len(chunk.Sessions))
	for i, session := range chunk.Sessions {
		sessionID := strings.TrimSpace(session.ID)
		if sessionID == "" {
			return fmt.Errorf("sessions[%d].id is required", i)
		}
		chunkSessionIDs[sessionID] = struct{}{}
	}
	for i, mutation := range chunk.Mutations {
		if mutation.Entity != store.SyncEntitySession || mutation.Op != store.SyncOpUpsert {
			continue
		}
		var body struct {
			ID string `json:"id"`
		}
		if err := decodeSyncMutationPayload(mutation.Payload, &body); err != nil {
			return fmt.Errorf("mutations[%d] invalid payload: %w", i, err)
		}
		sessionID := strings.TrimSpace(body.ID)
		if sessionID == "" {
			sessionID = strings.TrimSpace(mutation.EntityKey)
		}
		if sessionID == "" {
			return fmt.Errorf("mutations[%d].payload.id is required for session upsert", i)
		}
		chunkSessionIDs[sessionID] = struct{}{}
	}

	hasSession := func(sessionID string) bool {
		if _, ok := chunkSessionIDs[sessionID]; ok {
			return true
		}
		_, ok := knownSessionIDs[sessionID]
		return ok
	}

	for i, observation := range chunk.Observations {
		sessionID := strings.TrimSpace(observation.SessionID)
		if sessionID == "" {
			return fmt.Errorf("observations[%d].session_id is required", i)
		}
		if !hasSession(sessionID) {
			return fmt.Errorf("observations[%d] references missing session_id %q", i, sessionID)
		}
	}

	for i, prompt := range chunk.Prompts {
		sessionID := strings.TrimSpace(prompt.SessionID)
		if sessionID == "" {
			return fmt.Errorf("prompts[%d].session_id is required", i)
		}
		if !hasSession(sessionID) {
			return fmt.Errorf("prompts[%d] references missing session_id %q", i, sessionID)
		}
	}

	for i, mutation := range chunk.Mutations {
		if mutation.Entity != store.SyncEntityObservation && mutation.Entity != store.SyncEntityPrompt {
			continue
		}
		var body struct {
			SessionID string `json:"session_id"`
		}
		if err := decodeSyncMutationPayload(mutation.Payload, &body); err != nil {
			return fmt.Errorf("mutations[%d] invalid payload: %w", i, err)
		}
		sessionID := strings.TrimSpace(body.SessionID)
		if mutation.Op == store.SyncOpUpsert && sessionID == "" {
			return fmt.Errorf("mutations[%d].payload.session_id is required for upsert", i)
		}
		if mutation.Op == store.SyncOpUpsert && !hasSession(sessionID) {
			return fmt.Errorf("mutations[%d] references missing session_id %q", i, sessionID)
		}
	}
	return nil
}

func jsonResponse(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}
