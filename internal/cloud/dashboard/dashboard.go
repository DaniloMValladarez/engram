package dashboard

import (
	"fmt"
	"html"
	"net/http"

	"github.com/Gentleman-Programming/engram/internal/cloud/constants"
)

type SyncStatus struct {
	Phase         string
	ReasonCode    string
	ReasonMessage string
}

type SyncStatusProvider interface {
	Status() SyncStatus
}

type staticSyncStatusProvider struct {
	status SyncStatus
}

func (s staticSyncStatusProvider) Status() SyncStatus { return s.status }

func Handler() http.Handler {
	return HandlerWithStatus(staticSyncStatusProvider{status: SyncStatus{Phase: "idle"}})
}

func HandlerWithStatus(provider SyncStatusProvider) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		status := provider.Status()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(renderSyncStatusPage(status)))
	})
	return mux
}

func renderSyncStatusPage(status SyncStatus) string {
	code := status.ReasonCode
	message := status.ReasonMessage
	headline := reasonHeadline(status.ReasonCode)
	phase := status.Phase
	phase = html.EscapeString(phase)
	headline = html.EscapeString(headline)
	code = html.EscapeString(code)
	message = html.EscapeString(message)

	return fmt.Sprintf(`<html>
<head><title>Engram Cloud Dashboard</title></head>
<body>
  <main>
    <h1>Engram Cloud Dashboard</h1>
    <p>phase: %s</p>
    <section>
      <h2>%s</h2>
      <p>reason_code: %s</p>
      <p>reason_message: %s</p>
    </section>
  </main>
</body>
</html>`, phase, headline, code, message)
}

func reasonHeadline(code string) string {
	switch code {
	case constants.ReasonBlockedUnenrolled:
		return "Blocked — project unenrolled"
	case constants.ReasonPaused:
		return "Paused"
	case constants.ReasonAuthRequired:
		return "Authentication required"
	case constants.ReasonTransportFailed:
		return "Transport failure"
	default:
		if code == "" {
			return "Healthy"
		}
		return "Sync issue"
	}
}
