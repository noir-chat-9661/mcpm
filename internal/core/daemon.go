package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

type PSResponse struct {
	ConfigPath  string       `json:"configPath"`
	GatewayPort int          `json:"gatewayPort,omitempty"`
	DaemonPid   int          `json:"daemonPid"`
	Units       []UnitStatus `json:"units"`
}

type ControlRequest struct {
	Unit   string `json:"unit"`
	Reason string `json:"reason,omitempty"`
}

type Daemon struct {
	mu           sync.Mutex
	cfg          *Config
	units        map[string]*MCPUnit
	listeners    map[string][]net.Listener
	gatewayLn    []net.Listener
	logger       *log.Logger
	events       *EventLog
	shutdownFn   func()
	shuttingDown bool

	sseMu       sync.Mutex
	sseSeq      atomic.Uint64
	sseSessions map[string]chan []byte
}

func NewDaemon(cfg *Config, logger *log.Logger) *Daemon {
	d := &Daemon{
		cfg:         cfg,
		units:       map[string]*MCPUnit{},
		listeners:   map[string][]net.Listener{},
		logger:      logger,
		events:      &EventLog{},
		sseSessions: map[string]chan []byte{},
	}
	for _, name := range cfg.SortedServerNames() {
		sc := cfg.Servers[name]
		d.units[name] = NewMCPUnit(name, sc, cfg.IdleTimeout, logger)
	}
	return d
}

// RunDaemon は実プロセス（mcpmd）としてバックグラウンドまたは前面で常駐し、
// UDS および HTTP ポートを待ち受ける。TUI とは完全に独立したプロセスで動作する。
func RunDaemon(cfg *Config, warm []string) error {
	if err := EnsureHomeDir(); err != nil {
		return err
	}

	sock := SocketPath()
	if c, err := net.Dial("unix", sock); err == nil {
		_ = c.Close()
		return errors.New("mcpmd は既に起動しています")
	}
	_ = os.Remove(sock)

	d := NewDaemon(cfg, nil)
	var writers []io.Writer
	writers = append(writers, os.Stdout, d.events)
	if lf, err := os.OpenFile(filepath.Join(LogDir(), "_daemon.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600); err == nil {
		defer lf.Close()
		writers = append(writers, lf)
	}
	d.logger = log.New(io.MultiWriter(writers...), "", log.Ltime)
	for _, u := range d.units {
		u.logger = d.logger
	}

	if err := d.BindServerPorts(); err != nil {
		d.CloseListeners()
		return err
	}

	if cfg.GatewayPort > 0 {
		for _, host := range []string{"127.0.0.1", "::1"} {
			l, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(cfg.GatewayPort)))
			if err != nil {
				if host == "::1" && errors.Is(err, syscall.EADDRNOTAVAIL) {
					continue
				}
				d.logger.Printf("警告: Gateway ポート %d を待ち受けできませんでした: %v", cfg.GatewayPort, err)
				break
			}
			d.gatewayLn = append(d.gatewayLn, l)
			srv := &http.Server{Handler: d.GatewayHTTPHandler()}
			go srv.Serve(l)
		}
	}

	ctl, err := net.Listen("unix", sock)
	if err != nil {
		d.CloseListeners()
		return err
	}
	if err := os.Chmod(sock, 0o600); err != nil {
		d.CloseListeners()
		_ = ctl.Close()
		return err
	}
	ctlSrv := &http.Server{Handler: d.ControlHandler()}
	go ctlSrv.Serve(ctl)

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	d.shutdownFn = func() {
		select {
		case shutdown <- syscall.SIGTERM:
		default:
		}
	}

	d.logger.Printf("mcpmd 起動 (PID %d): %d 個の MCP サーバーを管理中", os.Getpid(), len(d.units))
	for _, name := range warm {
		u := d.ResolveUnit(name)
		if u == nil {
			continue
		}
		go func(unit *MCPUnit) {
			ctx, cancel := context.WithTimeout(context.Background(), ReadyTimeout)
			defer cancel()
			if _, err := unit.EnsureRunning(ctx, "事前起動 (--warm)", true); err != nil {
				d.logger.Printf("失敗 %s: %v", unit.name, err)
			}
		}(u)
	}

	<-shutdown

	d.logger.Printf("停止処理中")
	d.mu.Lock()
	d.shuttingDown = true
	units := make([]*MCPUnit, 0, len(d.units))
	for _, u := range d.units {
		u.mu.Lock()
		u.shuttingDown = true
		u.mu.Unlock()
		units = append(units, u)
	}
	d.mu.Unlock()

	_ = ctl.Close()
	_ = os.Remove(sock)
	d.CloseListeners()

	var wg sync.WaitGroup
	for _, u := range units {
		wg.Add(1)
		go func(unit *MCPUnit) {
			defer wg.Done()
			_ = unit.Stop("mcpmd 終了", true)
		}(u)
	}
	wg.Wait()
	return nil
}

func (d *Daemon) BindServerPorts() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	for name, u := range d.units {
		if u.cfg.Port <= 0 {
			continue
		}
		if len(d.listeners[name]) > 0 {
			continue
		}
		var lns []net.Listener
		for _, host := range []string{"127.0.0.1", "::1"} {
			l, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(u.cfg.Port)))
			if err != nil {
				if host == "::1" && errors.Is(err, syscall.EADDRNOTAVAIL) {
					continue
				}
				for _, prev := range lns {
					_ = prev.Close()
				}
				if errors.Is(err, syscall.EADDRINUSE) {
					return fmt.Errorf("ポート %d（%s）が既に使用されています", u.cfg.Port, name)
				}
				return err
			}
			lns = append(lns, l)
			srv := &http.Server{Handler: d.UnitHTTPHandler(u)}
			go srv.Serve(l)
		}
		d.listeners[name] = lns
	}
	return nil
}

func (d *Daemon) CloseListeners() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for name, lns := range d.listeners {
		for _, l := range lns {
			_ = l.Close()
		}
		delete(d.listeners, name)
	}
	for _, l := range d.gatewayLn {
		_ = l.Close()
	}
	d.gatewayLn = nil
}

func (d *Daemon) ResolveUnit(query string) *MCPUnit {
	d.mu.Lock()
	defer d.mu.Unlock()
	name := d.cfg.Resolve(query)
	if name == "" {
		if u, ok := d.units[query]; ok {
			return u
		}
		return nil
	}
	return d.units[name]
}

func (d *Daemon) Snapshot() PSResponse {
	d.mu.Lock()
	names := d.cfg.SortedServerNames()
	units := make([]UnitStatus, 0, len(names))
	for _, name := range names {
		if u, ok := d.units[name]; ok {
			units = append(units, u.Snapshot())
		}
	}
	gw := d.cfg.GatewayPort
	d.mu.Unlock()

	return PSResponse{
		ConfigPath:  ConfigPath(),
		GatewayPort: gw,
		DaemonPid:   os.Getpid(),
		Units:       units,
	}
}

func (d *Daemon) Act(action, query, reason string) error {
	if action == "reload" {
		return d.reloadConfig()
	}
	if action == "refresh" && (query == "" || query == "all") {
		d.mu.Lock()
		units := make([]*MCPUnit, 0, len(d.units))
		for _, u := range d.units {
			units = append(units, u)
		}
		d.mu.Unlock()
		for _, u := range units {
			ctx, cancel := context.WithTimeout(context.Background(), ReadyTimeout)
			_, err := u.EnsureCache(ctx, true)
			cancel()
			if err != nil {
				return err
			}
		}
		return nil
	}

	u := d.ResolveUnit(query)
	if u == nil {
		return fmt.Errorf("%q に該当する MCP サーバーまたはポートが見つかりません", query)
	}

	ctx, cancel := context.WithTimeout(context.Background(), ReadyTimeout)
	defer cancel()

	switch action {
	case "start", "up":
		if reason == "" {
			reason = "手動起動 (up)"
		}
		_, err := u.EnsureRunning(ctx, reason, true)
		return err
	case "stop", "close":
		if reason == "" {
			reason = "手動 close"
		}
		return u.Stop(reason, true)
	case "restart":
		if reason == "" {
			reason = "手動再起動"
		}
		return u.Restart(ctx, reason)
	case "refresh":
		_, err := u.EnsureCache(ctx, true)
		return err
	default:
		return fmt.Errorf("不明な操作: %s", action)
	}
}

func (d *Daemon) reloadConfig() error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.cfg = cfg
	seen := map[string]bool{}
	for _, name := range cfg.SortedServerNames() {
		seen[name] = true
		sc := cfg.Servers[name]
		if existing, ok := d.units[name]; ok {
			existing.mu.Lock()
			existing.cfg = sc
			existing.globalIdle = cfg.IdleTimeout
			existing.resetIdleTimerLocked()
			existing.mu.Unlock()
		} else {
			d.units[name] = NewMCPUnit(name, sc, cfg.IdleTimeout, d.logger)
		}
	}
	var removed []*MCPUnit
	for name, u := range d.units {
		if !seen[name] {
			removed = append(removed, u)
			delete(d.units, name)
			for _, l := range d.listeners[name] {
				_ = l.Close()
			}
			delete(d.listeners, name)
		}
	}
	d.mu.Unlock()

	for _, u := range removed {
		_ = u.Stop("設定から削除", true)
	}
	_ = d.BindServerPorts()
	if d.logger != nil {
		d.logger.Printf("設定を再読み込みしました（%d 個の MCP サーバー）", len(seen))
	}
	return nil
}

func (d *Daemon) ControlHandler() http.Handler {
	mux := http.NewServeMux()

	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	writeErr := func(w http.ResponseWriter, err error) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"Error": err.Error()})
	}

	mux.HandleFunc("GET /ps", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, d.Snapshot())
	})

	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		since, _ := strconv.Atoi(r.URL.Query().Get("since"))
		writeJSON(w, http.StatusOK, map[string]any{"Events": d.events.Since(since)})
	})

	for _, action := range []string{"start", "stop", "close", "restart", "refresh", "reload"} {
		actName := action
		mux.HandleFunc("POST /"+actName, func(w http.ResponseWriter, r *http.Request) {
			var req ControlRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if err := d.Act(actName, req.Unit, req.Reason); err != nil {
				writeErr(w, err)
				return
			}
			writeJSON(w, http.StatusOK, d.Snapshot())
		})
	}

	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"OK": true})
		if d.shutdownFn != nil {
			d.shutdownFn()
		}
	})

	mux.HandleFunc("POST /connect", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("name")
		u := d.ResolveUnit(name)
		if u == nil {
			writeErr(w, fmt.Errorf("MCP サーバー %q が登録されていません（`mcpm add` で登録してください）", name))
			return
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			writeErr(w, errors.New("hijack not supported"))
			return
		}
		conn, bufrw, err := hj.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()

		_, _ = bufrw.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson\r\n\r\n")
		_ = bufrw.Flush()

		d.serveRawStream(u, bufrw.Reader, conn)
	})

	return mux
}

func (d *Daemon) serveRawStream(u *MCPUnit, r *bufio.Reader, w io.Writer) {
	_, notifCh, unsubscribe := u.Subscribe()
	defer unsubscribe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var writeMu sync.Mutex
	sendLine := func(line []byte) error {
		if len(line) == 0 {
			return nil
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		_, err := w.Write(append(bytes.TrimRight(line, "\r\n"), '\n'))
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-notifCh:
				if !ok {
					return
				}
				_ = sendLine(msg)
			}
		}
	}()

	var reqWg sync.WaitGroup
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				msgCopy := append([]byte(nil), trimmed...)
				reqWg.Add(1)
				go func(raw []byte) {
					defer reqWg.Done()
					resp, hErr := u.HandleMessage(ctx, raw)
					if hErr == nil && len(resp) > 0 {
						_ = sendLine(resp)
					}
				}(msgCopy)
			}
		}
		if err != nil {
			break
		}
	}
	cancel()
	reqWg.Wait()
}

func (d *Daemon) GatewayHTTPHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		parts := strings.SplitN(path, "/", 3)
		if len(parts) < 2 {
			http.Error(w, "usage: /mcp/<server-name> or /sse/<server-name>", http.StatusNotFound)
			return
		}
		prefix, name := parts[0], parts[1]
		u := d.ResolveUnit(name)
		if u == nil {
			http.Error(w, fmt.Sprintf("unknown MCP server %q", name), http.StatusNotFound)
			return
		}
		switch prefix {
		case "mcp":
			d.serveStreamableHTTP(u, w, r)
		case "sse":
			d.serveSSE(u, "/message/"+u.name, w, r)
		case "message":
			d.serveSSEMessage(u, w, r)
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	})
}

func (d *Daemon) UnitHTTPHandler(u *MCPUnit) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sse":
			d.serveSSE(u, "/message", w, r)
		case "/message":
			d.serveSSEMessage(u, w, r)
		default:
			d.serveStreamableHTTP(u, w, r)
		}
	})
}

func (d *Daemon) serveStreamableHTTP(u *MCPUnit, w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		w.WriteHeader(http.StatusOK)
		return
	case http.MethodGet:
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		_, notifCh, unsubscribe := u.Subscribe()
		defer unsubscribe()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case msg, ok := <-notifCh:
				if !ok {
					return
				}
				_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", string(msg))
				flusher.Flush()
			}
		}
	case http.MethodPost:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		trimmed := bytes.TrimSpace(body)
		if len(trimmed) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		resp, err := u.HandleMessage(r.Context(), trimmed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if len(resp) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(append(resp, '\n'))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (d *Daemon) serveSSE(u *MCPUnit, msgPath string, w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	_, notifCh, unsubscribe := u.Subscribe()
	defer unsubscribe()

	sessID := strconv.FormatUint(d.sseSeq.Add(1), 10)
	respCh := make(chan []byte, 32)
	d.sseMu.Lock()
	d.sseSessions[sessID] = respCh
	d.sseMu.Unlock()
	defer func() {
		d.sseMu.Lock()
		delete(d.sseSessions, sessID)
		d.sseMu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, "event: endpoint\ndata: %s?sessionId=%s\n\n", msgPath, sessID)
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-notifCh:
			if !ok {
				return
			}
			_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", string(msg))
			flusher.Flush()
		case msg := <-respCh:
			_, _ = fmt.Fprintf(w, "event: message\ndata: %s\n\n", string(msg))
			flusher.Flush()
		}
	}
}

func (d *Daemon) serveSSEMessage(u *MCPUnit, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sessID := r.URL.Query().Get("sessionId")
	d.sseMu.Lock()
	respCh, ok := d.sseSessions[sessID]
	d.sseMu.Unlock()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusAccepted)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), ReadyTimeout*2)
		defer cancel()
		resp, hErr := u.HandleMessage(ctx, bytes.TrimSpace(body))
		if hErr == nil && len(resp) > 0 && ok {
			select {
			case respCh <- resp:
			default:
			}
		}
	}()
}
