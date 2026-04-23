package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Gentleman-Programming/engram/internal/cloud"
	"github.com/Gentleman-Programming/engram/internal/cloud/auth"
	"github.com/Gentleman-Programming/engram/internal/cloud/cloudserver"
	"github.com/Gentleman-Programming/engram/internal/cloud/cloudstore"
	"github.com/Gentleman-Programming/engram/internal/cloud/constants"
	"github.com/Gentleman-Programming/engram/internal/cloud/dashboard"
	"github.com/Gentleman-Programming/engram/internal/store"
	engramsync "github.com/Gentleman-Programming/engram/internal/sync"
)

type cloudManifestReader interface {
	ReadManifest(ctx context.Context, project string) (*engramsync.Manifest, error)
}

type cloudDashboardStatusProvider struct {
	store    cloudManifestReader
	projects []string
}

func (p cloudDashboardStatusProvider) Status() dashboard.SyncStatus {
	if len(p.projects) == 0 {
		return dashboard.SyncStatus{
			Phase:         "degraded",
			ReasonCode:    constants.ReasonBlockedUnenrolled,
			ReasonMessage: "cloud project allowlist is empty",
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	totalChunks := 0
	for _, project := range p.projects {
		manifest, err := p.store.ReadManifest(ctx, project)
		if err != nil {
			log.Printf("[engram] cloud dashboard status manifest read failed for project %q: %v", project, err)
			return dashboard.SyncStatus{
				Phase:         "degraded",
				ReasonCode:    constants.ReasonTransportFailed,
				ReasonMessage: "cloud sync status is temporarily unavailable",
			}
		}
		totalChunks += len(manifest.Chunks)
	}

	return dashboard.SyncStatus{
		Phase:         "healthy",
		ReasonMessage: fmt.Sprintf("cloud chunks available across %d project(s): %d", len(p.projects), totalChunks),
	}
}

type cloudServerRuntime interface {
	Start() error
}

type defaultCloudRuntime struct {
	server *cloudserver.CloudServer
	store  *cloudstore.CloudStore
}

func (r *defaultCloudRuntime) Start() error {
	defer r.store.Close()
	return r.server.Start()
}

var newCloudRuntime = func(cfg cloud.Config) (cloudServerRuntime, error) {
	cs, err := cloudstore.New(cfg)
	if err != nil {
		return nil, err
	}
	authSvc, err := auth.NewService(cs, cfg.JWTSecret)
	if err != nil {
		_ = cs.Close()
		return nil, err
	}
	allowedProjects := normalizeAllowedProjects(cfg.AllowedProjects)
	token := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_TOKEN"))
	authSvc.SetBearerToken(token)
	authSvc.SetAllowedProjects(allowedProjects)
	var authenticator cloudserver.Authenticator = authSvc
	if token == "" && envBool("ENGRAM_CLOUD_INSECURE_NO_AUTH") {
		authenticator = nil
	}
	return &defaultCloudRuntime{
		server: cloudserver.New(
			cs,
			authenticator,
			cfg.Port,
			cloudserver.WithHost(cfg.BindHost),
			cloudserver.WithProjectAuthorizer(authSvc),
			cloudserver.WithSyncStatusProvider(cloudDashboardStatusProvider{store: cs, projects: allowedProjects}),
		),
		store: cs,
	}, nil
}

type cloudConfig struct {
	ServerURL string `json:"server_url"`
	Token     string `json:"token"`
}

func cmdCloud(cfg store.Config) {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: engram cloud <subcommand> [options]")
		fmt.Fprintln(os.Stderr, "supported subcommands: status, enroll, config, serve")
		exitFunc(1)
	}

	switch os.Args[2] {
	case "status":
		cmdCloudStatus(cfg)
	case "enroll":
		cmdCloudEnroll(cfg)
	case "config":
		cmdCloudConfig(cfg)
	case "serve":
		cmdCloudServe()
	default:
		fmt.Fprintf(os.Stderr, "unknown cloud command: %s\n", os.Args[2])
		fmt.Fprintln(os.Stderr, "supported subcommands: status, enroll, config, serve")
		exitFunc(1)
	}
}

func cmdCloudStatus(cfg store.Config) {
	cc, err := resolveCloudRuntimeConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: unable to read cloud runtime config: %v\n", err)
		exitFunc(1)
		return
	}
	if cc == nil || cc.ServerURL == "" {
		fmt.Println("Cloud status: not configured")
		return
	}
	validatedURL, err := validateCloudServerURL(cc.ServerURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid cloud runtime server URL: %v\n", err)
		exitFunc(1)
		return
	}
	cc.ServerURL = validatedURL
	token := strings.TrimSpace(cc.Token)
	insecureNoAuth := envBool("ENGRAM_CLOUD_INSECURE_NO_AUTH")
	fmt.Printf("Cloud status: configured (target=%s)\n", constants.TargetKeyCloud)
	fmt.Printf("Server: %s\n", cc.ServerURL)
	if token == "" {
		if insecureNoAuth {
			fmt.Println("Auth status: ready (insecure local-dev mode: ENGRAM_CLOUD_INSECURE_NO_AUTH=1)")
			fmt.Println("Sync readiness: ready for explicit --project sync (project must be enrolled)")
			fmt.Println("Warning: bearer auth is disabled in insecure mode; do not use in production")
			return
		}
		fmt.Println("Auth status: token not configured (client token is optional at preflight)")
		fmt.Println("Sync readiness: ready to attempt explicit --project sync (project must be enrolled)")
		fmt.Println("Hint: if the remote server enforces bearer auth, set ENGRAM_CLOUD_TOKEN")
		return
	}
	fmt.Println("Auth status: ready (token provided via runtime cloud config)")
	fmt.Println("Sync readiness: ready for explicit --project sync (project must be enrolled)")
}

func cmdCloudEnroll(cfg store.Config) {
	if len(os.Args) < 4 || strings.TrimSpace(os.Args[3]) == "" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud enroll <project>")
		exitFunc(1)
	}

	s, err := storeNew(cfg)
	if err != nil {
		fatal(err)
		return
	}
	defer s.Close()

	projectName := strings.TrimSpace(os.Args[3])
	if err := s.EnrollProject(projectName); err != nil {
		fatal(err)
		return
	}

	fmt.Printf("✓ Project %q enrolled for cloud sync\n", projectName)
}

func cmdCloudConfig(cfg store.Config) {
	if len(os.Args) < 5 || os.Args[3] != "--server" {
		fmt.Fprintln(os.Stderr, "usage: engram cloud config --server <url>")
		exitFunc(1)
	}
	cc := &cloudConfig{ServerURL: strings.TrimSpace(os.Args[4])}
	if cc.ServerURL == "" {
		fmt.Fprintln(os.Stderr, "error: server URL is required")
		exitFunc(1)
	}
	validatedURL, err := validateCloudServerURL(cc.ServerURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: invalid server URL: %v\n", err)
		exitFunc(1)
	}
	cc.ServerURL = validatedURL
	if err := saveCloudConfig(cfg, cc); err != nil {
		fatal(err)
		return
	}
	fmt.Printf("✓ Cloud server set to %s\n", cc.ServerURL)
}

func validateCloudServerURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.ParseRequestURI(trimmed)
	if err != nil {
		return "", err
	}
	scheme := strings.ToLower(strings.TrimSpace(parsed.Scheme))
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("scheme must be http or https")
	}
	if strings.TrimSpace(parsed.Host) == "" || strings.TrimSpace(parsed.Hostname()) == "" {
		return "", fmt.Errorf("host is required")
	}
	if strings.TrimSpace(parsed.RawQuery) != "" {
		return "", fmt.Errorf("query is not allowed")
	}
	if strings.TrimSpace(parsed.Fragment) != "" {
		return "", fmt.Errorf("fragment is not allowed")
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func cmdCloudServe() {
	runtimeCfg := cloud.ConfigFromEnv()
	if err := validateCloudServeAuthConfig(); err != nil {
		fatal(err)
		return
	}
	runtime, err := newCloudRuntime(runtimeCfg)
	if err != nil {
		fatal(err)
		return
	}
	fmt.Printf("Starting Engram cloud server on port %d\n", runtimeCfg.Port)
	if err := runtime.Start(); err != nil {
		fatal(err)
	}
}

func validateCloudServeAuthConfig() error {
	token := strings.TrimSpace(os.Getenv("ENGRAM_CLOUD_TOKEN"))
	insecureNoAuth := envBool("ENGRAM_CLOUD_INSECURE_NO_AUTH")
	allowlist := normalizeAllowedProjects(cloud.ConfigFromEnv().AllowedProjects)
	jwtSecretEnv := strings.TrimSpace(os.Getenv("ENGRAM_JWT_SECRET"))
	if insecureNoAuth && token != "" {
		return fmt.Errorf("conflicting cloud auth configuration: ENGRAM_CLOUD_INSECURE_NO_AUTH=1 cannot be used together with ENGRAM_CLOUD_TOKEN")
	}
	if token != "" && len(allowlist) > 0 {
		if jwtSecretEnv == "" {
			return fmt.Errorf("authenticated cloud serve requires explicit ENGRAM_JWT_SECRET (non-default); refusing implicit default secret")
		}
		if cloud.IsDefaultJWTSecret(jwtSecretEnv) {
			return fmt.Errorf("authenticated cloud serve requires a non-default ENGRAM_JWT_SECRET; refusing development default")
		}
		return nil
	}
	if insecureNoAuth {
		if len(allowlist) == 0 {
			return fmt.Errorf("cloud project allowlist is required even in insecure mode: set ENGRAM_CLOUD_ALLOWED_PROJECTS to one or more project names")
		}
		fmt.Fprintln(os.Stderr, "warning: ENGRAM_CLOUD_INSECURE_NO_AUTH=1 disables cloud API authentication; do not use in production")
		return nil
	}
	if token == "" {
		return fmt.Errorf("cloud auth token is required: set ENGRAM_CLOUD_TOKEN (or ENGRAM_CLOUD_INSECURE_NO_AUTH=1 for local insecure development)")
	}
	return fmt.Errorf("cloud project allowlist is required: set ENGRAM_CLOUD_ALLOWED_PROJECTS to one or more project names")
}

func normalizeAllowedProjects(projects []string) []string {
	normalized := make([]string, 0, len(projects))
	seen := make(map[string]struct{})
	for _, project := range projects {
		name, _ := store.NormalizeProject(project)
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		normalized = append(normalized, name)
	}
	return normalized
}

func cloudConfigPath(cfg store.Config) string {
	return filepath.Join(cfg.DataDir, "cloud.json")
}

func loadCloudConfig(cfg store.Config) (*cloudConfig, error) {
	path := cloudConfigPath(cfg)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cc cloudConfig
	if err := json.Unmarshal(b, &cc); err != nil {
		return nil, err
	}
	return &cc, nil
}

func saveCloudConfig(cfg store.Config, cc *cloudConfig) error {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(cc, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(cloudConfigPath(cfg), b, 0o644)
}
