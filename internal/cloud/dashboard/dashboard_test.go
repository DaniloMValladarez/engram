package dashboard

import (
	"html"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type stubSyncStatusProvider struct {
	status SyncStatus
}

func (s stubSyncStatusProvider) Status() SyncStatus { return s.status }

func TestHandlerWithStatusRendersDeterministicReasonParity(t *testing.T) {
	tests := []struct {
		name          string
		reasonCode    string
		reasonMessage string
		expectedLabel string
	}{
		{
			name:          "blocked unenrolled reason",
			reasonCode:    "blocked_unenrolled",
			reasonMessage: "project \"alpha\" is not enrolled for cloud sync",
			expectedLabel: "Blocked — project unenrolled",
		},
		{
			name:          "auth required reason",
			reasonCode:    "auth_required",
			reasonMessage: "cloud credentials are missing: configure server URL and token",
			expectedLabel: "Authentication required",
		},
		{
			name:          "transport failed reason",
			reasonCode:    "transport_failed",
			reasonMessage: "dial tcp 127.0.0.1:443: connect: connection refused",
			expectedLabel: "Transport failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := HandlerWithStatus(stubSyncStatusProvider{status: SyncStatus{
				Phase:         "degraded",
				ReasonCode:    tt.reasonCode,
				ReasonMessage: tt.reasonMessage,
			}})

			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", rr.Code)
			}

			body := rr.Body.String()
			if !strings.Contains(body, tt.reasonCode) {
				t.Fatalf("expected body to contain reason code %q, body=%q", tt.reasonCode, body)
			}
			if !strings.Contains(html.UnescapeString(body), tt.reasonMessage) {
				t.Fatalf("expected body to contain reason message %q, body=%q", tt.reasonMessage, body)
			}
			if !strings.Contains(body, tt.expectedLabel) {
				t.Fatalf("expected body to contain reason label %q, body=%q", tt.expectedLabel, body)
			}
		})
	}
}

func TestHandlerWithStatusEscapesDynamicFields(t *testing.T) {
	h := HandlerWithStatus(stubSyncStatusProvider{status: SyncStatus{
		Phase:         `<script>alert("p")</script>`,
		ReasonCode:    `<b>code</b>`,
		ReasonMessage: `<img src=x onerror=alert(1)>`,
	}})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	body := rr.Body.String()
	forbidden := []string{"<script>", "<img", "<b>code</b>"}
	for _, token := range forbidden {
		if strings.Contains(body, token) {
			t.Fatalf("expected escaped output without %q, body=%q", token, body)
		}
	}
	if !strings.Contains(body, "&lt;script&gt;alert") {
		t.Fatalf("expected script tag to be escaped, body=%q", body)
	}
}
