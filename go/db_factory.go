// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package presto

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	presto "github.com/prestodb/presto-go-client/v2"
)

// Response header timeout for the underlying HTTP transport.  The Presto
// protocol polls the server with short requests, so each individual response
// should start quickly even for long-running queries.  This guards against
// infrastructure/network hangs without limiting total query runtime.
const defaultResponseHeaderTimeout = 2 * time.Minute

// PrestoDBFactory provides Presto-specific database connection creation.
// It handles Presto DSN formatting and connection parameters.
type PrestoDBFactory struct{}

// NewPrestoDBFactory creates a new PrestoDBFactory.
func NewPrestoDBFactory() *PrestoDBFactory {
	return &PrestoDBFactory{}
}

// prestoDSN holds the parsed connection settings extracted from ADBC options.
type prestoDSN struct {
	// url is the presto:// DSN handed to the presto-go-client.
	url *url.URL
	// catalog and schema are the initial namespace from the URI path.
	catalog, schema string
	// useTLS indicates the connection should use HTTPS.
	useTLS bool
	// sslCA, sslCert, sslKey are paths to PEM files for TLS configuration.
	sslCA, sslCert, sslKey string
	// sslSkipVerify disables server certificate verification.
	sslSkipVerify bool
}

// namespaceState tracks the current catalog/schema for a database.
//
// PrestoDB has no current_catalog/current_schema SQL functions (unlike
// Trino), and the presto go client does not expose its per-connection
// session, so the driver tracks the current namespace itself.  The state is
// applied to every new connection through presto.WithSessionSetup, keeping
// pooled connections aligned with the tracked namespace.
type namespaceState struct {
	mu      sync.Mutex
	catalog string
	schema  string
}

func (s *namespaceState) get() (catalog, schema string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.catalog, s.schema
}

func (s *namespaceState) setCatalog(catalog string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catalog = catalog
}

func (s *namespaceState) setSchema(schema string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schema = schema
}

// dbNamespaces maps a *sql.DB created by CreateDB to its namespace state.
// Entries live for the lifetime of the process; one entry exists per opened
// ADBC database, so growth is bounded by database churn.
var dbNamespaces sync.Map

// namespaceForDB returns the namespace state for a database created by
// PrestoDBFactory, or an empty fallback if none is registered.
func namespaceForDB(db *sql.DB) *namespaceState {
	if state, ok := dbNamespaces.Load(db); ok {
		return state.(*namespaceState)
	}
	// Should not happen for databases created by CreateDB; return a
	// throwaway state so callers degrade gracefully.
	return &namespaceState{}
}

// CreateDB creates a *sql.DB from the provided ADBC options.
//
// Rather than sql.Open with a plain DSN, this uses presto.NewConnector with a
// custom HTTP client so that TLS configuration and timeouts are fully
// controlled by the driver.
func (f *PrestoDBFactory) CreateDB(ctx context.Context, driverName string, opts map[string]string, logger *slog.Logger) (*sql.DB, error) {
	cfg, err := f.buildPrestoDSN(opts)
	if err != nil {
		return nil, err
	}

	httpClient, err := f.buildHTTPClient(cfg)
	if err != nil {
		return nil, err
	}

	state := &namespaceState{catalog: cfg.catalog, schema: cfg.schema}
	connector, err := presto.NewConnector(cfg.url.String(),
		presto.WithHTTPClient(httpClient),
		// Apply the tracked namespace to every new connection so that
		// changes via SetCurrentCatalog/SetCurrentDbSchema propagate to
		// pooled connections.
		presto.WithSessionSetup(func(session *presto.Session) {
			catalog, schema := state.get()
			if catalog != "" {
				session.Catalog(catalog)
			}
			if schema != "" {
				session.Schema(schema)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Presto connector: %w", err)
	}

	db := sql.OpenDB(connector)
	dbNamespaces.Store(db, state)
	return db, nil
}

// buildHTTPClient constructs the HTTP client injected into the Presto
// connector.  The presto-go-client applies WithHTTPClient after any DSN-based
// TLS configuration, so the TLS settings on this client's transport are
// authoritative.
func (f *PrestoDBFactory) buildHTTPClient(cfg *prestoDSN) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = defaultResponseHeaderTimeout

	if cfg.useTLS {
		tlsConfig, err := buildTLSConfig(transport.TLSClientConfig, cfg)
		if err != nil {
			return nil, err
		}
		transport.TLSClientConfig = tlsConfig
	}

	return &http.Client{Transport: transport}, nil
}

// buildTLSConfig constructs the TLS configuration for the custom transport.
func buildTLSConfig(base *tls.Config, cfg *prestoDSN) (*tls.Config, error) {
	var tlsConfig *tls.Config
	if base == nil {
		tlsConfig = &tls.Config{}
	} else {
		tlsConfig = base.Clone()
	}

	if cfg.sslSkipVerify {
		tlsConfig.InsecureSkipVerify = true
	}

	if cfg.sslCA != "" {
		pem, err := os.ReadFile(cfg.sslCA)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		certPool, err := rootCertPool(tlsConfig.RootCAs)
		if err != nil {
			return nil, err
		}
		if !certPool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", cfg.sslCA)
		}
		tlsConfig.RootCAs = certPool
	}

	if cfg.sslCert != "" && cfg.sslKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.sslCert, cfg.sslKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func rootCertPool(base *x509.CertPool) (*x509.CertPool, error) {
	if base != nil {
		return base.Clone(), nil
	}

	certPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("failed to load system cert pool: %w", err)
	}
	if certPool == nil {
		return x509.NewCertPool(), nil
	}
	return certPool, nil
}

// BuildPrestoDSN constructs a presto:// DSN string from the provided ADBC
// options.  Exposed for testing and diagnostics; CreateDB uses the richer
// internal form that also carries TLS configuration.
func (f *PrestoDBFactory) BuildPrestoDSN(opts map[string]string) (string, error) {
	cfg, err := f.buildPrestoDSN(opts)
	if err != nil {
		return "", err
	}
	return cfg.url.String(), nil
}

// buildPrestoDSN constructs a Presto DSN from the provided options.
// Handles the following scenarios:
//  1. Presto URI: "presto://user:pass@host:port/catalog/schema?params"
//     → passed through, with credential overrides applied.
//  2. HTTP(S) URI: "https://user:pass@host:8443/catalog/schema?params"
//     → converted to presto:// form; https enables TLS.
//  3. Plain host + credentials: baseURI="localhost:8080", username="user"
//     → produces "presto://user@localhost:8080".
func (f *PrestoDBFactory) buildPrestoDSN(opts map[string]string) (*prestoDSN, error) {
	baseURI := opts[adbc.OptionKeyURI]
	username := opts[adbc.OptionKeyUsername]
	password := opts[adbc.OptionKeyPassword]

	if baseURI == "" {
		// Return plain Go error. sqlwrapper will catch and wrap it with
		// ErrorHelper and turn it into an adbc error.
		return nil, fmt.Errorf("missing required option %s", adbc.OptionKeyURI)
	}

	// Bare host (no scheme): default to presto:// (HTTP).
	if !strings.Contains(baseURI, "://") {
		baseURI = "presto://" + baseURI
	}

	u, err := url.Parse(baseURI)
	if err != nil {
		return nil, fmt.Errorf("invalid URI format: %v", err)
	}

	cfg := &prestoDSN{}
	queryParams := u.Query()

	switch strings.ToLower(u.Scheme) {
	case "presto":
		// Native scheme: TLS is implied by any ssl_* parameter.
	case "http":
		u.Scheme = "presto"
	case "https":
		u.Scheme = "presto"
		cfg.useTLS = true
	default:
		return nil, fmt.Errorf("unsupported URI scheme %q: must be presto, http, or https", u.Scheme)
	}

	// Extract TLS parameters.  They are removed from the DSN because the
	// custom HTTP client owns TLS configuration; ssl_skip_verify is re-added
	// below purely to signal HTTPS to the presto-go-client.
	cfg.sslCA = queryParams.Get("ssl_ca")
	cfg.sslCert = queryParams.Get("ssl_cert")
	cfg.sslKey = queryParams.Get("ssl_key")
	skip := queryParams.Get("ssl_skip_verify")
	cfg.sslSkipVerify = strings.EqualFold(skip, "true") || skip == "1"
	if cfg.sslCA != "" || cfg.sslCert != "" || cfg.sslKey != "" || cfg.sslSkipVerify {
		cfg.useTLS = true
	}
	queryParams.Del("ssl_ca")
	queryParams.Del("ssl_cert")
	queryParams.Del("ssl_key")
	queryParams.Del("ssl_skip_verify")

	if cfg.useTLS {
		// The presto-go-client selects https only when an ssl_* DSN parameter
		// is present.  The actual TLS behavior comes from the injected HTTP
		// client (applied after DSN TLS config), so this parameter only
		// controls the URL scheme.
		queryParams.Set("ssl_skip_verify", "true")
	}

	u.User = applyCredentialOverrides(u.User, username, password)
	u.RawQuery = queryParams.Encode()

	// Record the initial namespace from the URI path (/catalog[/schema]).
	if path := strings.TrimPrefix(u.Path, "/"); path != "" {
		parts := strings.SplitN(path, "/", 2)
		cfg.catalog = parts[0]
		if len(parts) == 2 && parts[1] != "" {
			cfg.schema = parts[1]
		}
	}

	if u.Hostname() == "" {
		return nil, fmt.Errorf("missing host in URI")
	}
	if u.Port() == "" {
		defaultPort := "8080"
		if cfg.useTLS {
			defaultPort = "8443"
		}
		// net.JoinHostPort re-brackets IPv6 hosts correctly.
		u.Host = net.JoinHostPort(u.Hostname(), defaultPort)
	}

	cfg.url = u
	return cfg, nil
}

// applyCredentialOverrides contains username and password override logic
func applyCredentialOverrides(existing *url.Userinfo, username, password string) *url.Userinfo {
	if username == "" && password == "" {
		return existing
	}

	user := ""
	pass := ""
	hasPass := false

	if existing != nil {
		user = existing.Username()
		pass, hasPass = existing.Password()
	}

	if username != "" {
		user = username
	}
	if password != "" {
		pass = password
		hasPass = true
	}

	if hasPass {
		return url.UserPassword(user, pass)
	}
	return url.User(user)
}

// Ensure PrestoDBFactory implements sqlwrapper.DBFactory
var _ sqlwrapper.DBFactory = (*PrestoDBFactory)(nil)
