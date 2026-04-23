package cloudserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	cloudauth "github.com/Gentleman-Programming/engram/internal/cloud/auth"
	"github.com/Gentleman-Programming/engram/internal/cloud/cloudstore"
	"github.com/Gentleman-Programming/engram/internal/cloud/dashboard"
	"github.com/Gentleman-Programming/engram/internal/store"
	engramsync "github.com/Gentleman-Programming/engram/internal/sync"
)

type fakeStore struct {
	manifest        engramsync.Manifest
	chunks          map[string][]byte
	sessions        map[string]map[string]struct{}
	errRead         error
	errWrite        error
	project         string
	clientCreatedAt string
}

func (s *fakeStore) ReadManifest(context.Context, string) (*engramsync.Manifest, error) {
	m := s.manifest
	return &m, nil
}

func (s *fakeStore) WriteChunk(_ context.Context, project string, chunkID, createdBy, clientCreatedAt string, payload []byte) error {
	if s.errWrite != nil {
		return s.errWrite
	}
	s.project = project
	s.clientCreatedAt = clientCreatedAt
	if s.chunks == nil {
		s.chunks = make(map[string][]byte)
	}
	s.chunks[chunkID] = append([]byte(nil), payload...)
	s.manifest.Chunks = append(s.manifest.Chunks, engramsync.ChunkEntry{ID: chunkID, CreatedBy: createdBy, CreatedAt: clientCreatedAt})
	var chunk engramsync.ChunkData
	if err := json.Unmarshal(payload, &chunk); err == nil {
		if s.sessions == nil {
			s.sessions = make(map[string]map[string]struct{})
		}
		if _, ok := s.sessions[project]; !ok {
			s.sessions[project] = make(map[string]struct{})
		}
		for _, sess := range chunk.Sessions {
			if strings.TrimSpace(sess.ID) != "" {
				s.sessions[project][strings.TrimSpace(sess.ID)] = struct{}{}
			}
		}
	}
	return nil
}

func TestHandlerPushRejectsInvalidClientCreatedAt(t *testing.T) {
	srv := New(&fakeStore{}, fakeAuth{}, 0)
	body := bytes.NewBufferString(`{"project":"proj-a","created_by":"tester","client_created_at":"not-a-time","data":{"sessions":[{"id":"s-1","directory":"/tmp/s-1"}]}}`)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "client_created_at") {
		t.Fatalf("expected client_created_at validation error, got %q", rec.Body.String())
	}
}

func (s *fakeStore) ReadChunk(_ context.Context, _ string, chunkID string) ([]byte, error) {
	if s.errRead != nil {
		return nil, s.errRead
	}
	v, ok := s.chunks[chunkID]
	if !ok {
		return nil, fmt.Errorf("%w", cloudstore.ErrChunkNotFound)
	}
	return append([]byte(nil), v...), nil
}

func (s *fakeStore) KnownSessionIDs(_ context.Context, project string) (map[string]struct{}, error) {
	known := make(map[string]struct{})
	if s.sessions == nil {
		return known, nil
	}
	for sessionID := range s.sessions[project] {
		known[sessionID] = struct{}{}
	}
	return known, nil
}

type fakeAuth struct {
	err        error
	projectErr error
}

func (a fakeAuth) Authorize(*http.Request) error { return a.err }
func (a fakeAuth) AuthorizeProject(string) error { return a.projectErr }

type strictBearerAuth struct{ token string }

func (a strictBearerAuth) Authorize(r *http.Request) error {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if header == "" {
		return fmt.Errorf("missing authorization header")
	}
	if header != "Bearer "+a.token {
		return fmt.Errorf("invalid bearer token")
	}
	return nil
}

type staticStatus struct{ status dashboard.SyncStatus }

func (s staticStatus) Status() dashboard.SyncStatus { return s.status }

func TestHandlerMountsDashboardAndHealth(t *testing.T) {
	srv := New(&fakeStore{}, fakeAuth{}, 0, WithSyncStatusProvider(staticStatus{status: dashboard.SyncStatus{
		Phase:         "degraded",
		ReasonCode:    "auth_required",
		ReasonMessage: "token missing",
	}}))

	health := httptest.NewRecorder()
	srv.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("expected /health=200, got %d", health.Code)
	}

	dashboardRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(dashboardRec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	if dashboardRec.Code != http.StatusOK {
		t.Fatalf("expected /dashboard=200, got %d", dashboardRec.Code)
	}
	if !strings.Contains(dashboardRec.Body.String(), "reason_code: auth_required") {
		t.Fatalf("expected dashboard to render reason parity, body=%q", dashboardRec.Body.String())
	}
}

func TestHandlerSyncPushPullRoundTrip(t *testing.T) {
	st := &fakeStore{}
	srv := New(st, fakeAuth{}, 0)

	payload := []byte(`{"sessions":[{"id":"s-1","directory":"/tmp/s-1"}]}`)
	normalizedPayload, err := coerceChunkProject(payload, "proj-a")
	if err != nil {
		t.Fatalf("coerce payload: %v", err)
	}
	chunkID := chunkIDFromPayload(normalizedPayload)
	clientCreatedAt := "2026-04-01T12:30:00Z"
	body := bytes.NewBufferString(`{"chunk_id":"` + chunkID + `","project":"proj-a","created_by":"tester","client_created_at":"` + clientCreatedAt + `","data":` + string(payload) + `}`)
	pushRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(pushRec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if pushRec.Code != http.StatusOK {
		t.Fatalf("expected /sync/push=200, got %d body=%q", pushRec.Code, pushRec.Body.String())
	}

	pullManifest := httptest.NewRecorder()
	srv.Handler().ServeHTTP(pullManifest, httptest.NewRequest(http.MethodGet, "/sync/pull?project=proj-a", nil))
	if pullManifest.Code != http.StatusOK {
		t.Fatalf("expected /sync/pull=200, got %d", pullManifest.Code)
	}
	var manifest engramsync.Manifest
	if err := json.Unmarshal(pullManifest.Body.Bytes(), &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if len(manifest.Chunks) != 1 || manifest.Chunks[0].ID != chunkID {
		t.Fatalf("unexpected manifest %+v", manifest.Chunks)
	}
	if manifest.Chunks[0].CreatedAt != clientCreatedAt {
		t.Fatalf("expected manifest to preserve client_created_at, got %+v", manifest.Chunks[0])
	}

	pullChunk := httptest.NewRecorder()
	srv.Handler().ServeHTTP(pullChunk, httptest.NewRequest(http.MethodGet, "/sync/pull/"+chunkID+"?project=proj-a", nil))
	if pullChunk.Code != http.StatusOK {
		t.Fatalf("expected /sync/pull/chunk-1=200, got %d", pullChunk.Code)
	}
	if string(bytes.TrimSpace(pullChunk.Body.Bytes())) != string(normalizedPayload) {
		t.Fatalf("unexpected chunk body=%q", pullChunk.Body.String())
	}
}

func TestHandlerReturnsUnauthorizedWhenAuthFails(t *testing.T) {
	srv := New(&fakeStore{}, fakeAuth{err: errors.New("bad token")}, 0)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/sync/pull?project=proj-a", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	dashboardRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(dashboardRec, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	if dashboardRec.Code != http.StatusSeeOther {
		t.Fatalf("expected /dashboard redirect to login, got %d", dashboardRec.Code)
	}
	if location := dashboardRec.Header().Get("Location"); location != "/dashboard/login" {
		t.Fatalf("expected redirect location /dashboard/login, got %q", location)
	}
}

func TestHandlerDashboardLoginFlowSetsCookieForBrowserUse(t *testing.T) {
	authSvc, err := cloudauth.NewService(&cloudstore.CloudStore{}, strings.Repeat("x", 32))
	if err != nil {
		t.Fatalf("new auth service: %v", err)
	}
	authSvc.SetBearerToken("secret-token")
	srv := New(&fakeStore{}, authSvc, 0)

	unauth := httptest.NewRecorder()
	srv.Handler().ServeHTTP(unauth, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
	if unauth.Code != http.StatusSeeOther {
		t.Fatalf("expected unauthenticated dashboard request to redirect, got %d", unauth.Code)
	}

	loginPage := httptest.NewRecorder()
	srv.Handler().ServeHTTP(loginPage, httptest.NewRequest(http.MethodGet, "/dashboard/login", nil))
	if loginPage.Code != http.StatusOK {
		t.Fatalf("expected /dashboard/login=200, got %d", loginPage.Code)
	}
	if !strings.Contains(loginPage.Body.String(), "Dashboard Login") {
		t.Fatalf("expected dashboard login page html, body=%q", loginPage.Body.String())
	}

	badLogin := httptest.NewRecorder()
	badReq := httptest.NewRequest(http.MethodPost, "/dashboard/login", strings.NewReader("token=wrong"))
	badReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.Handler().ServeHTTP(badLogin, badReq)
	if badLogin.Code != http.StatusOK {
		t.Fatalf("expected invalid login attempt to re-render form with 200, got %d", badLogin.Code)
	}
	if !strings.Contains(badLogin.Body.String(), "invalid token") {
		t.Fatalf("expected invalid token message, body=%q", badLogin.Body.String())
	}

	login := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/dashboard/login", strings.NewReader("token=secret-token"))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.Handler().ServeHTTP(login, loginReq)
	if login.Code != http.StatusSeeOther {
		t.Fatalf("expected successful login redirect, got %d", login.Code)
	}
	if location := login.Header().Get("Location"); location != "/dashboard" {
		t.Fatalf("expected redirect to /dashboard, got %q", location)
	}
	cookies := login.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("expected dashboard session cookie to be set")
	}

	dashboard := httptest.NewRecorder()
	dashboardReq := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	for _, cookie := range cookies {
		dashboardReq.AddCookie(cookie)
	}
	srv.Handler().ServeHTTP(dashboard, dashboardReq)
	if dashboard.Code != http.StatusOK {
		t.Fatalf("expected dashboard request with cookie to succeed, got %d", dashboard.Code)
	}
}

func TestHandlerDashboardLoginRejectsTokenFromQueryString(t *testing.T) {
	authSvc, err := cloudauth.NewService(&cloudstore.CloudStore{}, strings.Repeat("x", 32))
	if err != nil {
		t.Fatalf("new auth service: %v", err)
	}
	authSvc.SetBearerToken("secret-token")
	srv := New(&fakeStore{}, authSvc, 0)

	login := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/dashboard/login?token=secret-token", strings.NewReader(""))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.Handler().ServeHTTP(login, loginReq)

	if login.Code != http.StatusOK {
		t.Fatalf("expected login form re-render when token is query-sourced, got %d", login.Code)
	}
	if !strings.Contains(login.Body.String(), "token is required") {
		t.Fatalf("expected token required error, got body=%q", login.Body.String())
	}
	for _, cookie := range login.Result().Cookies() {
		if cookie.Name == dashboardSessionCookieName {
			t.Fatal("expected no session cookie when token is passed via query string")
		}
	}
}

func TestHandlerDashboardLoginRejectsOversizedFormPayload(t *testing.T) {
	authSvc, err := cloudauth.NewService(&cloudstore.CloudStore{}, strings.Repeat("x", 32))
	if err != nil {
		t.Fatalf("new auth service: %v", err)
	}
	authSvc.SetBearerToken("secret-token")
	srv := New(&fakeStore{}, authSvc, 0)

	oversizedToken := strings.Repeat("x", int(maxDashboardLoginBodyBytes)+1)
	login := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/dashboard/login", strings.NewReader("token="+oversizedToken))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.Handler().ServeHTTP(login, loginReq)

	if login.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized login payload, got %d body=%q", login.Code, login.Body.String())
	}
	if !strings.Contains(login.Body.String(), "payload too large") {
		t.Fatalf("expected clear payload too large error, got %q", login.Body.String())
	}
}

func TestHandlerDashboardLoginCookieSecureRespectsForwardedProto(t *testing.T) {
	tests := []struct {
		name           string
		forwardedProto string
		wantSecure     bool
	}{
		{name: "plain http request", forwardedProto: "", wantSecure: false},
		{name: "proxy terminated tls", forwardedProto: "https", wantSecure: true},
		{name: "multi proto header", forwardedProto: "http, https", wantSecure: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authSvc, err := cloudauth.NewService(&cloudstore.CloudStore{}, strings.Repeat("x", 32))
			if err != nil {
				t.Fatalf("new auth service: %v", err)
			}
			authSvc.SetBearerToken("secret-token")
			srv := New(&fakeStore{}, authSvc, 0)

			login := httptest.NewRecorder()
			loginReq := httptest.NewRequest(http.MethodPost, "/dashboard/login", strings.NewReader("token=secret-token"))
			loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if tt.forwardedProto != "" {
				loginReq.Header.Set("X-Forwarded-Proto", tt.forwardedProto)
			}
			srv.Handler().ServeHTTP(login, loginReq)
			if login.Code != http.StatusSeeOther {
				t.Fatalf("expected successful login redirect, got %d", login.Code)
			}

			var sessionCookie *http.Cookie
			for _, cookie := range login.Result().Cookies() {
				if cookie.Name == dashboardSessionCookieName {
					sessionCookie = cookie
					break
				}
			}
			if sessionCookie == nil {
				t.Fatal("expected dashboard session cookie")
			}
			if sessionCookie.Secure != tt.wantSecure {
				t.Fatalf("expected secure=%v, got %v", tt.wantSecure, sessionCookie.Secure)
			}
		})
	}
}

func TestHandlerDashboardLoginFailsClosedWithoutSessionCodec(t *testing.T) {
	srv := New(&fakeStore{}, strictBearerAuth{token: "secret-token"}, 0)

	login := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/dashboard/login", strings.NewReader("token=secret-token"))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.Handler().ServeHTTP(login, loginReq)
	if login.Code != http.StatusInternalServerError {
		t.Fatalf("expected login to fail closed without session codec, got %d", login.Code)
	}
	for _, cookie := range login.Result().Cookies() {
		if cookie.Name == dashboardSessionCookieName {
			t.Fatal("expected no dashboard session cookie when session codec is missing")
		}
	}

	dashboard := httptest.NewRecorder()
	dashboardReq := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	dashboardReq.AddCookie(&http.Cookie{Name: dashboardSessionCookieName, Value: "secret-token"})
	srv.Handler().ServeHTTP(dashboard, dashboardReq)
	if dashboard.Code != http.StatusSeeOther {
		t.Fatalf("expected dashboard to reject raw bearer cookie fallback, got %d", dashboard.Code)
	}
	if location := dashboard.Header().Get("Location"); location != "/dashboard/login" {
		t.Fatalf("expected redirect to /dashboard/login, got %q", location)
	}
}

func TestHandlerDashboardLoginUsesSignedSessionCookieWithAuthService(t *testing.T) {
	authSvc, err := cloudauth.NewService(&cloudstore.CloudStore{}, strings.Repeat("x", 32))
	if err != nil {
		t.Fatalf("new auth service: %v", err)
	}
	authSvc.SetBearerToken("secret-token")
	srv := New(&fakeStore{}, authSvc, 0)

	login := httptest.NewRecorder()
	loginReq := httptest.NewRequest(http.MethodPost, "/dashboard/login", strings.NewReader("token=secret-token"))
	loginReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	srv.Handler().ServeHTTP(login, loginReq)
	if login.Code != http.StatusSeeOther {
		t.Fatalf("expected successful login redirect, got %d", login.Code)
	}

	var sessionCookie *http.Cookie
	for _, cookie := range login.Result().Cookies() {
		if cookie.Name == dashboardSessionCookieName {
			sessionCookie = cookie
			break
		}
	}
	if sessionCookie == nil {
		t.Fatal("expected dashboard session cookie")
	}
	if sessionCookie.Value == "secret-token" {
		t.Fatal("expected signed dashboard session cookie value, got raw bearer token")
	}

	dashboard := httptest.NewRecorder()
	dashboardReq := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	dashboardReq.AddCookie(sessionCookie)
	srv.Handler().ServeHTTP(dashboard, dashboardReq)
	if dashboard.Code != http.StatusOK {
		t.Fatalf("expected dashboard request with signed cookie to succeed, got %d", dashboard.Code)
	}
}

func TestHandlerPullChunkMapsInternalErrorsTo500(t *testing.T) {
	st := &fakeStore{errRead: errors.New("db down")}
	srv := New(st, fakeAuth{}, 0)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/sync/pull/chunk-1?project=proj-a", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandlerRequiresProjectScope(t *testing.T) {
	srv := New(&fakeStore{}, fakeAuth{}, 0)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/sync/pull", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing project, got %d", rec.Code)
	}
}

func TestHandlerRejectsNormalizedEmptyProjectScope(t *testing.T) {
	srv := New(&fakeStore{}, fakeAuth{}, 0)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/sync/pull?project=%20%20%20", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for normalized-empty project, got %d", rec.Code)
	}
}

func TestHandlerPushRejectsNormalizedEmptyProject(t *testing.T) {
	srv := New(&fakeStore{}, fakeAuth{}, 0)
	body := bytes.NewBufferString(`{"project":"   ","created_by":"tester","data":{"sessions":[{"id":"s-1","directory":"/tmp/s-1"}]}}`)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for normalized-empty push project, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandlerPushRejectsChunkIDPayloadMismatch(t *testing.T) {
	srv := New(&fakeStore{}, fakeAuth{}, 0)
	body := bytes.NewBufferString(`{"chunk_id":"deadbeef","project":"proj-a","created_by":"tester","data":{"sessions":[{"id":"s-1"}]}}`)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandlerPushMapsChunkConflictTo409(t *testing.T) {
	st := &fakeStore{errWrite: cloudstore.ErrChunkConflict}
	srv := New(st, fakeAuth{}, 0)
	payload := []byte(`{"sessions":[{"id":"s-1","directory":"/tmp/s-1"}]}`)
	normalizedPayload, err := coerceChunkProject(payload, "proj-a")
	if err != nil {
		t.Fatalf("coerce payload: %v", err)
	}
	chunkID := chunkIDFromPayload(normalizedPayload)
	body := bytes.NewBufferString(`{"chunk_id":"` + chunkID + `","project":"proj-a","created_by":"tester","data":` + string(payload) + `}`)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandlerPushRejectsOversizedPayload(t *testing.T) {
	srv := New(&fakeStore{}, fakeAuth{}, 0)
	tooLarge := strings.Repeat("x", int(maxPushBodyBytes)+1)
	body := bytes.NewBufferString(`{"project":"proj-a","created_by":"tester","data":"` + tooLarge + `"}`)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "payload too large") {
		t.Fatalf("expected clear oversized payload error, got body=%q", rec.Body.String())
	}
}

func TestHandlerRejectsProjectOutsideTokenScope(t *testing.T) {
	srv := New(&fakeStore{}, fakeAuth{projectErr: errors.New("project denied")}, 0)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/sync/pull?project=proj-a", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%q", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "project denied") {
		t.Fatalf("expected generic forbidden response, got body=%q", rec.Body.String())
	}
}

func TestHandlerEnforcesProjectAllowlistWithoutBearerAuth(t *testing.T) {
	srv := New(&fakeStore{}, nil, 0, WithProjectAuthorizer(fakeAuth{projectErr: errors.New("project \"proj-c\" is not allowed for this token (allowed: proj-a,proj-b)")}))
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/sync/pull?project=proj-a", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 with auth disabled but allowlist enforced, got %d body=%q", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "proj-a") || strings.Contains(rec.Body.String(), "proj-b") {
		t.Fatalf("forbidden response must not leak allowlist details, got body=%q", rec.Body.String())
	}
}

func TestHandlerPushRewritesEmbeddedProjectToRequestProject(t *testing.T) {
	st := &fakeStore{}
	srv := New(st, fakeAuth{}, 0)
	payload := []byte(`{"sessions":[{"id":"s-1","project":"other","directory":"/tmp/s-1"}],"observations":[{"sync_id":"obs-1","session_id":"s-1","type":"decision","title":"t","content":"c","scope":"project","project":"different"}],"prompts":[{"sync_id":"prompt-1","session_id":"s-1","content":"hello","project":"third"}]}`)
	normalizedPayload, err := coerceChunkProject(payload, "proj-a")
	if err != nil {
		t.Fatalf("coerce payload: %v", err)
	}
	chunkID := chunkIDFromPayload(normalizedPayload)
	body := bytes.NewBufferString(`{"chunk_id":"` + chunkID + `","project":"proj-a","created_by":"tester","data":` + string(payload) + `}`)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%q", rec.Code, rec.Body.String())
	}
	if st.project != "proj-a" {
		t.Fatalf("expected normalized request project proj-a, got %q", st.project)
	}

	var got map[string]any
	if err := json.Unmarshal(st.chunks[chunkID], &got); err != nil {
		t.Fatalf("decode stored payload: %v", err)
	}
	for _, key := range []string{"sessions", "observations", "prompts"} {
		items, ok := got[key].([]any)
		if !ok || len(items) == 0 {
			t.Fatalf("expected non-empty %s array in stored payload", key)
		}
		entry, ok := items[0].(map[string]any)
		if !ok {
			t.Fatalf("expected object in %s[0]", key)
		}
		if entry["project"] != "proj-a" {
			t.Fatalf("expected %s[0].project to be rewritten, got %v", key, entry["project"])
		}
	}
}

func TestHandlerPushRejectsSchemaInvalidChunkPayload(t *testing.T) {
	st := &fakeStore{}
	srv := New(st, fakeAuth{}, 0)

	// sessions[0].id must be a string in importable chunk schema.
	body := bytes.NewBufferString(`{"project":"proj-a","created_by":"tester","data":{"sessions":[{"id":123}]}}`)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "chunk schema") {
		t.Fatalf("expected schema validation error, got body=%q", rec.Body.String())
	}
	if len(st.chunks) != 0 {
		t.Fatalf("expected no chunk writes for invalid payload, got %d", len(st.chunks))
	}
}

func TestHandlerPushRejectsUnsupportedMutationEntityOp(t *testing.T) {
	st := &fakeStore{}
	srv := New(st, fakeAuth{}, 0)

	body := bytes.NewBufferString(`{"project":"proj-a","created_by":"tester","data":{"mutations":[{"entity":"session","entity_key":"s-1","op":"delete","payload":"{\"id\":\"s-1\",\"project\":\"wrong\"}"}]}}`)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unsupported mutation") {
		t.Fatalf("expected unsupported mutation error, got body=%q", rec.Body.String())
	}
	if len(st.chunks) != 0 {
		t.Fatalf("expected no chunk writes for invalid mutations, got %d", len(st.chunks))
	}
}

func TestHandlerPushRewritesMutationPayloadProjectToRequestProject(t *testing.T) {
	st := &fakeStore{}
	srv := New(st, fakeAuth{}, 0)

	payload := []byte(`{"sessions":[{"id":"s-1","project":"other","directory":"/tmp","started_at":"2026-04-10T08:00:00Z"}],"mutations":[{"entity":"session","entity_key":"s-1","op":"upsert","project":"wrong","payload":"{\"id\":\"s-1\",\"project\":\"still-wrong\",\"directory\":\"/tmp\",\"started_at\":\"2026-04-10T08:00:00Z\"}"},{"entity":"observation","entity_key":"obs-1","op":"upsert","project":"wrong","payload":"{\"sync_id\":\"obs-1\",\"session_id\":\"s-1\",\"type\":\"note\",\"title\":\"meta\",\"content\":\"payload\",\"scope\":\"project\",\"project\":\"still-wrong\",\"created_at\":\"2026-04-10T08:01:00Z\",\"updated_at\":\"2026-04-10T08:02:00Z\",\"last_seen_at\":\"2026-04-10T08:03:00Z\",\"revision_count\":7,\"duplicate_count\":3}"}]}`)
	normalizedPayload, err := coerceChunkProject(payload, "proj-a")
	if err != nil {
		t.Fatalf("coerce payload: %v", err)
	}
	chunkID := chunkIDFromPayload(normalizedPayload)
	body := bytes.NewBufferString(`{"chunk_id":"` + chunkID + `","project":"proj-a","created_by":"tester","data":` + string(payload) + `}`)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%q", rec.Code, rec.Body.String())
	}

	var stored engramsync.ChunkData
	if err := json.Unmarshal(st.chunks[chunkID], &stored); err != nil {
		t.Fatalf("decode stored chunk: %v", err)
	}
	if len(stored.Mutations) != 2 {
		t.Fatalf("expected two mutations, got %d", len(stored.Mutations))
	}
	if stored.Mutations[0].Project != "proj-a" {
		t.Fatalf("expected mutation project rewritten to proj-a, got %q", stored.Mutations[0].Project)
	}

	var mutationPayload map[string]any
	if err := json.Unmarshal([]byte(stored.Mutations[0].Payload), &mutationPayload); err != nil {
		t.Fatalf("decode mutation payload: %v", err)
	}
	if mutationPayload["project"] != "proj-a" {
		t.Fatalf("expected mutation payload project rewritten, got %v", mutationPayload["project"])
	}
	if mutationPayload["started_at"] != "2026-04-10T08:00:00Z" {
		t.Fatalf("expected session started_at to be preserved, got %v", mutationPayload["started_at"])
	}

	var observationPayload map[string]any
	if err := json.Unmarshal([]byte(stored.Mutations[1].Payload), &observationPayload); err != nil {
		t.Fatalf("decode observation mutation payload: %v", err)
	}
	if observationPayload["project"] != "proj-a" {
		t.Fatalf("expected observation project rewritten, got %v", observationPayload["project"])
	}
	if observationPayload["created_at"] != "2026-04-10T08:01:00Z" {
		t.Fatalf("expected observation created_at preserved, got %v", observationPayload["created_at"])
	}
	if observationPayload["updated_at"] != "2026-04-10T08:02:00Z" {
		t.Fatalf("expected observation updated_at preserved, got %v", observationPayload["updated_at"])
	}
	if observationPayload["last_seen_at"] != "2026-04-10T08:03:00Z" {
		t.Fatalf("expected observation last_seen_at preserved, got %v", observationPayload["last_seen_at"])
	}
	if observationPayload["revision_count"] != float64(7) {
		t.Fatalf("expected observation revision_count preserved, got %v", observationPayload["revision_count"])
	}
	if observationPayload["duplicate_count"] != float64(3) {
		t.Fatalf("expected observation duplicate_count preserved, got %v", observationPayload["duplicate_count"])
	}
}

func TestHandlerPushRejectsObservationReferencingUnknownSession(t *testing.T) {
	st := &fakeStore{}
	srv := New(st, fakeAuth{}, 0)

	payload := []byte(`{"observations":[{"sync_id":"obs-missing","session_id":"missing","type":"note","title":"t","content":"c","scope":"project"}]}`)
	normalizedPayload, err := coerceChunkProject(payload, "proj-a")
	if err != nil {
		t.Fatalf("coerce payload: %v", err)
	}
	chunkID := chunkIDFromPayload(normalizedPayload)
	body := bytes.NewBufferString(`{"chunk_id":"` + chunkID + `","project":"proj-a","created_by":"tester","data":` + string(payload) + `}`)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "missing session_id") {
		t.Fatalf("expected referential integrity error, got body=%q", rec.Body.String())
	}
}

func TestHandlerPushAcceptsReferencesToSessionsAlreadyInRemoteHistory(t *testing.T) {
	st := &fakeStore{}
	srv := New(st, fakeAuth{}, 0)

	seedPayload := []byte(`{"sessions":[{"id":"s-existing","directory":"/tmp/s-existing"}]}`)
	normalizedSeed, err := coerceChunkProject(seedPayload, "proj-a")
	if err != nil {
		t.Fatalf("coerce seed payload: %v", err)
	}
	seedChunkID := chunkIDFromPayload(normalizedSeed)
	seedBody := bytes.NewBufferString(`{"chunk_id":"` + seedChunkID + `","project":"proj-a","created_by":"tester","data":` + string(seedPayload) + `}`)
	seedRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(seedRec, httptest.NewRequest(http.MethodPost, "/sync/push", seedBody))
	if seedRec.Code != http.StatusOK {
		t.Fatalf("expected seed push 200, got %d body=%q", seedRec.Code, seedRec.Body.String())
	}

	obsPayload := []byte(`{"observations":[{"sync_id":"obs-2","session_id":"s-existing","type":"note","title":"next","content":"payload","scope":"project"}]}`)
	normalizedObs, err := coerceChunkProject(obsPayload, "proj-a")
	if err != nil {
		t.Fatalf("coerce observation payload: %v", err)
	}
	obsChunkID := chunkIDFromPayload(normalizedObs)
	obsBody := bytes.NewBufferString(`{"chunk_id":"` + obsChunkID + `","project":"proj-a","created_by":"tester","data":` + string(obsPayload) + `}`)
	obsRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(obsRec, httptest.NewRequest(http.MethodPost, "/sync/push", obsBody))
	if obsRec.Code != http.StatusOK {
		t.Fatalf("expected second push 200, got %d body=%q", obsRec.Code, obsRec.Body.String())
	}
}

func TestHandlerPushAcceptsDeleteMutationWithoutKnownSession(t *testing.T) {
	st := &fakeStore{}
	srv := New(st, fakeAuth{}, 0)

	payload := []byte(`{"mutations":[{"entity":"observation","entity_key":"obs-1","op":"delete","payload":"{\"sync_id\":\"obs-1\",\"deleted\":true,\"hard_delete\":true}"}]}`)
	normalizedPayload, err := coerceChunkProject(payload, "proj-a")
	if err != nil {
		t.Fatalf("coerce payload: %v", err)
	}
	chunkID := chunkIDFromPayload(normalizedPayload)
	body := bytes.NewBufferString(`{"chunk_id":"` + chunkID + `","project":"proj-a","created_by":"tester","data":` + string(payload) + `}`)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandlerPushAcceptsMutationReferencesToSessionUpsertInSameChunk(t *testing.T) {
	st := &fakeStore{}
	srv := New(st, fakeAuth{}, 0)

	payload := []byte(`{"mutations":[{"entity":"session","entity_key":"s-new","op":"upsert","payload":"{\"id\":\"s-new\",\"directory\":\"/tmp/s-new\"}"},{"entity":"observation","entity_key":"obs-2","op":"upsert","payload":"{\"sync_id\":\"obs-2\",\"session_id\":\"s-new\",\"type\":\"decision\",\"title\":\"t\",\"content\":\"c\",\"scope\":\"project\"}"}]}`)
	normalizedPayload, err := coerceChunkProject(payload, "proj-a")
	if err != nil {
		t.Fatalf("coerce payload: %v", err)
	}
	chunkID := chunkIDFromPayload(normalizedPayload)
	body := bytes.NewBufferString(`{"chunk_id":"` + chunkID + `","project":"proj-a","created_by":"tester","data":` + string(payload) + `}`)

	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%q", rec.Code, rec.Body.String())
	}
}

func TestHandlerPushRejectsMutationUpsertsMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{
			name:    "session upsert missing directory",
			payload: `{"mutations":[{"entity":"session","entity_key":"s-1","op":"upsert","payload":"{\"id\":\"s-1\"}"}]}`,
			wantErr: "session payload directory is required for upsert",
		},
		{
			name:    "observation upsert missing title",
			payload: `{"mutations":[{"entity":"observation","entity_key":"obs-1","op":"upsert","payload":"{\"sync_id\":\"obs-1\",\"session_id\":\"s-1\",\"type\":\"decision\",\"content\":\"c\",\"scope\":\"project\"}"}]}`,
			wantErr: "observation payload title is required for upsert",
		},
		{
			name:    "prompt upsert missing content",
			payload: `{"mutations":[{"entity":"prompt","entity_key":"p-1","op":"upsert","payload":"{\"sync_id\":\"p-1\",\"session_id\":\"s-1\"}"}]}`,
			wantErr: "prompt payload content is required for upsert",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &fakeStore{sessions: map[string]map[string]struct{}{"proj-a": {"s-1": struct{}{}}}}
			srv := New(st, fakeAuth{}, 0)
			body := bytes.NewBufferString(`{"project":"proj-a","created_by":"tester","data":` + tt.payload + `}`)

			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d body=%q", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tt.wantErr) {
				t.Fatalf("expected error %q, got body=%q", tt.wantErr, rec.Body.String())
			}
		})
	}
}

func TestHandlerPushRejectsDirectChunkArraysMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{
			name:    "session missing directory",
			payload: `{"sessions":[{"id":"s-1"}]}`,
			wantErr: "sessions[0].directory is required",
		},
		{
			name:    "observation missing sync_id",
			payload: `{"sessions":[{"id":"s-1","directory":"/tmp/s-1"}],"observations":[{"session_id":"s-1","type":"decision","title":"t","content":"c","scope":"project"}]}`,
			wantErr: "observations[0].sync_id is required",
		},
		{
			name:    "prompt missing content",
			payload: `{"sessions":[{"id":"s-1","directory":"/tmp/s-1"}],"prompts":[{"sync_id":"p-1","session_id":"s-1"}]}`,
			wantErr: "prompts[0].content is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &fakeStore{}
			srv := New(st, fakeAuth{}, 0)
			body := bytes.NewBufferString(`{"project":"proj-a","created_by":"tester","data":` + tt.payload + `}`)

			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/sync/push", body))
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d body=%q", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tt.wantErr) {
				t.Fatalf("expected error %q, got body=%q", tt.wantErr, rec.Body.String())
			}
			if len(st.chunks) != 0 {
				t.Fatalf("expected no chunk writes for invalid direct chunk payload, got %d", len(st.chunks))
			}
		})
	}
}

func TestValidateChunkSessionReferencesAcceptsDeleteMutationWithoutSession(t *testing.T) {
	err := validateChunkSessionReferences(engramsync.ChunkData{Mutations: []store.SyncMutation{{
		Entity:  store.SyncEntityObservation,
		Op:      store.SyncOpDelete,
		Payload: `{"sync_id":"obs-1","deleted":true}`,
	}}}, map[string]struct{}{})
	if err != nil {
		t.Fatalf("expected delete-only mutation to pass session validation, got %v", err)
	}
}

func TestStartBindsConfiguredHost(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		expected string
	}{
		{name: "loopback default", host: "", expected: "127.0.0.1:18080"},
		{name: "container friendly", host: "0.0.0.0", expected: "0.0.0.0:18080"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := New(&fakeStore{}, fakeAuth{}, 18080)
			srv.host = tt.host

			var gotAddr string
			srv.listenAndServe = func(addr string, _ http.Handler) error {
				gotAddr = addr
				return fmt.Errorf("stop")
			}

			err := srv.Start()
			if err == nil || err.Error() != "stop" {
				t.Fatalf("expected sentinel error, got %v", err)
			}
			if gotAddr != tt.expected {
				t.Fatalf("expected addr %q, got %q", tt.expected, gotAddr)
			}
		})
	}
}
