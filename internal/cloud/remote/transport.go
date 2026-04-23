package remote

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Gentleman-Programming/engram/internal/cloud/chunkcodec"
	"github.com/Gentleman-Programming/engram/internal/store"
	engramsync "github.com/Gentleman-Programming/engram/internal/sync"
)

type RemoteTransport struct {
	baseURL    string
	token      string
	project    string
	httpClient *http.Client
}

type HTTPStatusError struct {
	Operation  string
	StatusCode int
	Body       string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("cloud: %s: status %d: %s", e.Operation, e.StatusCode, strings.TrimSpace(e.Body))
}

func (e *HTTPStatusError) IsAuthFailure() bool {
	return e != nil && e.StatusCode == http.StatusUnauthorized
}

func (e *HTTPStatusError) IsPolicyFailure() bool {
	return e != nil && e.StatusCode == http.StatusForbidden
}

func newHTTPStatusError(operation string, statusCode int, body []byte) error {
	return &HTTPStatusError{
		Operation:  operation,
		StatusCode: statusCode,
		Body:       strings.TrimSpace(string(body)),
	}
}

func NewRemoteTransport(baseURL, token, project string) (*RemoteTransport, error) {
	normalized, err := validateBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	project, _ = store.NormalizeProject(project)
	project = strings.TrimSpace(project)
	if project == "" {
		return nil, fmt.Errorf("cloud: project is required")
	}
	return &RemoteTransport{
		baseURL: normalized,
		token:   strings.TrimSpace(token),
		project: project,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func validateBaseURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("cloud: remote url is required")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("cloud: invalid remote url: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("cloud: invalid remote url: scheme must be http or https")
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("cloud: invalid remote url: host is required")
	}
	if strings.TrimSpace(parsed.RawQuery) != "" {
		return "", fmt.Errorf("cloud: invalid remote url: query is not allowed")
	}
	if strings.TrimSpace(parsed.Fragment) != "" {
		return "", fmt.Errorf("cloud: invalid remote url: fragment is not allowed")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return strings.TrimRight(parsed.String(), "/"), nil
}

func (rt *RemoteTransport) endpointURL(query url.Values, parts ...string) (string, error) {
	endpoint, err := url.JoinPath(rt.baseURL, parts...)
	if err != nil {
		return "", fmt.Errorf("cloud: build request url: %w", err)
	}
	if len(query) == 0 {
		return endpoint, nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("cloud: build request url: %w", err)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (rt *RemoteTransport) setAuthorization(req *http.Request) {
	if rt.token == "" {
		return
	}
	req.Header.Set("Authorization", "Bearer "+rt.token)
}

func (rt *RemoteTransport) ReadManifest() (*engramsync.Manifest, error) {
	reqURL, err := rt.endpointURL(url.Values{"project": []string{rt.project}}, "sync", "pull")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("cloud: build manifest request: %w", err)
	}
	rt.setAuthorization(req)

	resp, err := rt.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cloud: fetch manifest: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, newHTTPStatusError("fetch manifest", resp.StatusCode, body)
	}

	var m engramsync.Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("cloud: parse manifest: %w", err)
	}
	return &m, nil
}

func (rt *RemoteTransport) WriteManifest(_ *engramsync.Manifest) error {
	return nil
}

func (rt *RemoteTransport) WriteChunk(chunkID string, data []byte, entry engramsync.ChunkEntry) error {
	canonicalData, err := chunkcodec.CanonicalizeForProject(data, rt.project)
	if err != nil {
		return fmt.Errorf("cloud: canonicalize push chunk: %w", err)
	}
	canonicalChunkID := chunkcodec.ChunkID(canonicalData)
	if strings.TrimSpace(chunkID) != "" && strings.TrimSpace(chunkID) != canonicalChunkID {
		chunkID = canonicalChunkID
	}

	body, err := json.Marshal(map[string]any{
		"chunk_id":          canonicalChunkID,
		"created_by":        entry.CreatedBy,
		"client_created_at": strings.TrimSpace(entry.CreatedAt),
		"project":           rt.project,
		"data":              json.RawMessage(canonicalData),
	})
	if err != nil {
		return fmt.Errorf("cloud: marshal push request: %w", err)
	}
	pushURL, err := rt.endpointURL(nil, "sync", "push")
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, pushURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cloud: build push request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	rt.setAuthorization(req)

	resp, err := rt.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("cloud: push chunk %s: %w", chunkID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return newHTTPStatusError(fmt.Sprintf("push chunk %s", chunkID), resp.StatusCode, body)
	}
	return nil
}

func (rt *RemoteTransport) ReadChunk(chunkID string) ([]byte, error) {
	reqURL, err := rt.endpointURL(url.Values{"project": []string{rt.project}}, "sync", "pull", chunkID)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("cloud: build pull request: %w", err)
	}
	rt.setAuthorization(req)
	resp, err := rt.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cloud: pull chunk %s: %w", chunkID, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, engramsync.ErrChunkNotFound
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, newHTTPStatusError(fmt.Sprintf("pull chunk %s", chunkID), resp.StatusCode, body)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cloud: read chunk %s response: %w", chunkID, err)
	}
	if len(data) == 0 {
		return nil, errors.New("cloud: empty chunk payload")
	}
	return data, nil
}

type MutationEntry struct {
	Entity    string          `json:"entity"`
	EntityKey string          `json:"entity_key"`
	Op        string          `json:"op"`
	Payload   json.RawMessage `json:"payload"`
}

type PushMutationsResult struct{}

type PulledMutation struct {
	Seq        int64           `json:"seq"`
	Entity     string          `json:"entity"`
	EntityKey  string          `json:"entity_key"`
	Op         string          `json:"op"`
	Payload    json.RawMessage `json:"payload"`
	OccurredAt string          `json:"occurred_at"`
}

type PullMutationsResponse struct {
	Mutations []PulledMutation `json:"mutations"`
}

func (rt *RemoteTransport) PushMutations(_ []MutationEntry) (*PushMutationsResult, error) {
	return nil, fmt.Errorf("cloud: mutation push is not available in this release")
}

func (rt *RemoteTransport) PullMutations(_ int64, _ int) (*PullMutationsResponse, error) {
	return nil, fmt.Errorf("cloud: mutation pull is not available in this release")
}
