package core

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultIdleTimeout = 5 * time.Minute
	DefaultGatewayPort = 18800
)

// Config は mcpm 全体の設定ファイル（~/.config/mcpm/config.json）。
type Config struct {
	GatewayPort int                     `json:"gatewayPort,omitempty"`
	IdleTimeout string                  `json:"idleTimeout,omitempty"`
	Servers     map[string]ServerConfig `json:"mcpServers"`
}

// ServerConfig は管理対象の 1 つの MCP サーバー設定。
type ServerConfig struct {
	Command           string            `json:"command"`
	Args              []string          `json:"args,omitempty"`
	Env               map[string]string `json:"env,omitempty"`
	Cwd               string            `json:"cwd,omitempty"`
	Port              int               `json:"port,omitempty"`
	IdleTimeout       string            `json:"idleTimeout,omitempty"`
	CloseOnDisconnect *bool             `json:"closeOnDisconnect,omitempty"`
	Disabled          bool              `json:"disabled,omitempty"`
}

func (s ServerConfig) EffectiveIdleTimeout(global string) time.Duration {
	raw := strings.TrimSpace(s.IdleTimeout)
	if raw == "" {
		raw = strings.TrimSpace(global)
	}
	if raw == "" {
		return DefaultIdleTimeout
	}
	if raw == "0" || strings.EqualFold(raw, "off") || strings.EqualFold(raw, "none") {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < 0 {
		return DefaultIdleTimeout
	}
	return d
}

func (s ServerConfig) ShouldCloseOnDisconnect() bool {
	if s.CloseOnDisconnect == nil {
		return true
	}
	return *s.CloseOnDisconnect
}

// SpecHash はスキーマやプロセス実体に影響する設定（Command, Args, Env, Cwd）のハッシュ値を返す。
func (s ServerConfig) SpecHash() string {
	type spec struct {
		Command string            `json:"command"`
		Args    []string          `json:"args,omitempty"`
		Env     map[string]string `json:"env,omitempty"`
		Cwd     string            `json:"cwd,omitempty"`
	}
	b, _ := json.Marshal(spec{
		Command: s.Command,
		Args:    s.Args,
		Env:     s.Env,
		Cwd:     s.Cwd,
	})
	sum := sha256.Sum256(b)
	return fmt.Sprintf("%x", sum[:16])
}

func HomeDir() string {
	if v := os.Getenv("MCPM_HOME"); v != "" {
		return v
	}
	h, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "mcpm")
	}
	return filepath.Join(h, ".config", "mcpm")
}

func ConfigPath() string {
	if v := os.Getenv("MCPM_CONFIG"); v != "" {
		return v
	}
	return filepath.Join(HomeDir(), "config.json")
}

func SocketPath() string {
	p := filepath.Join(HomeDir(), "mcpm.sock")
	if len(p) <= 100 {
		return p
	}
	sum := sha256.Sum256([]byte(HomeDir()))
	return filepath.Join("/tmp", fmt.Sprintf("mcpm-%x.sock", sum[:6]))
}

func CacheDir() string {
	return filepath.Join(HomeDir(), "cache")
}

func LogDir() string {
	return filepath.Join(HomeDir(), "logs")
}

func EnsureHomeDir() error {
	for _, dir := range []string{HomeDir(), CacheDir(), LogDir()} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return err
		}
		_ = os.Chmod(dir, 0o700)
	}
	return nil
}

func LoadConfig() (*Config, error) {
	if err := EnsureHomeDir(); err != nil {
		return nil, err
	}
	p := ConfigPath()
	data, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &Config{
				GatewayPort: DefaultGatewayPort,
				Servers:     map[string]ServerConfig{},
			}, nil
		}
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("設定ファイル (%s) の読み込みに失敗しました: %w", p, err)
	}
	if cfg.Servers == nil {
		cfg.Servers = map[string]ServerConfig{}
	}
	return &cfg, nil
}

func SaveConfig(cfg *Config) error {
	if err := EnsureHomeDir(); err != nil {
		return err
	}
	if cfg.Servers == nil {
		cfg.Servers = map[string]ServerConfig{}
	}
	data, err := json.MarshalIndent(cfg, "", "\t")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(ConfigPath(), data, 0o600)
}

func (c *Config) SortedServerNames() []string {
	names := make([]string, 0, len(c.Servers))
	for name, sc := range c.Servers {
		if sc.Disabled {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (c *Config) Resolve(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return ""
	}
	if sc, ok := c.Servers[query]; ok && !sc.Disabled {
		return query
	}
	if port, err := strconv.Atoi(query); err == nil && port > 0 {
		for name, sc := range c.Servers {
			if !sc.Disabled && sc.Port == port {
				return name
			}
		}
	}
	return ""
}
