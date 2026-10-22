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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	ReadyTimeout       = 45 * time.Second
	StopGrace          = 3 * time.Second
	FailBackoff        = 10 * time.Second
	DisconnectGrace    = 1500 * time.Millisecond
	DefaultProtocolVer = "2024-11-05"
)

type startResult struct {
	done chan struct{}
	err  error
}

type backendProc struct {
	procMu     sync.Mutex
	cmd        *exec.Cmd
	pid        int
	stdin      io.WriteCloser
	logFile    *os.File
	stopping   bool
	cancelInit context.CancelFunc
	specHash   string

	writeMu   sync.Mutex
	exited    chan struct{}
	closeOnce sync.Once
	exitErr   error

	seq      atomic.Uint64
	pendMu   sync.Mutex
	pending  map[uint64]chan RPCMessage
	initRaw  json.RawMessage
	readyRes *startResult
}

func (bp *backendProc) requestStop() (io.WriteCloser, int) {
	bp.procMu.Lock()
	defer bp.procMu.Unlock()
	bp.stopping = true
	if bp.cancelInit != nil {
		bp.cancelInit()
	}
	return bp.stdin, bp.pid
}

func (bp *backendProc) isStopping() bool {
	bp.procMu.Lock()
	defer bp.procMu.Unlock()
	return bp.stopping
}

func (bp *backendProc) markExited(err error) {
	bp.closeOnce.Do(func() {
		bp.exitErr = err
		close(bp.exited)
		bp.pendMu.Lock()
		for id, ch := range bp.pending {
			close(ch)
			delete(bp.pending, id)
		}
		bp.pendMu.Unlock()
		bp.procMu.Lock()
		lf := bp.logFile
		bp.logFile = nil
		bp.procMu.Unlock()
		if lf != nil {
			_ = lf.Close()
		}
	})
}

func (bp *backendProc) sendRaw(line []byte) error {
	bp.writeMu.Lock()
	defer bp.writeMu.Unlock()
	select {
	case <-bp.exited:
		return errors.New("MCP プロセスは既に終了しています")
	default:
	}
	bp.procMu.Lock()
	stdin := bp.stdin
	bp.procMu.Unlock()
	if stdin == nil {
		return errors.New("MCP プロセスの標準入力が未初期化です")
	}
	if _, err := stdin.Write(append(bytes.TrimRight(line, "\r\n"), '\n')); err != nil {
		return err
	}
	return nil
}

func (bp *backendProc) call(ctx context.Context, method string, params json.RawMessage) (RPCMessage, error) {
	seqID := bp.seq.Add(1)
	ch := make(chan RPCMessage, 1)

	bp.pendMu.Lock()
	select {
	case <-bp.exited:
		bp.pendMu.Unlock()
		return RPCMessage{}, errors.New("MCP プロセスは既に終了しています")
	default:
	}
	bp.pending[seqID] = ch
	bp.pendMu.Unlock()

	defer func() {
		bp.pendMu.Lock()
		delete(bp.pending, seqID)
		bp.pendMu.Unlock()
	}()

	idBytes, _ := json.Marshal(seqID)
	req := RPCMessage{
		JSONRPC: "2.0",
		ID:      idBytes,
		Method:  method,
		Params:  params,
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return RPCMessage{}, err
	}
	if err := bp.sendRaw(raw); err != nil {
		return RPCMessage{}, err
	}

	select {
	case <-ctx.Done():
		cancelParams, _ := json.Marshal(map[string]any{"requestId": seqID})
		cancelNotif, _ := json.Marshal(RPCMessage{
			JSONRPC: "2.0",
			Method:  "notifications/cancelled",
			Params:  cancelParams,
		})
		_ = bp.sendRaw(cancelNotif)
		return RPCMessage{}, ctx.Err()
	case <-bp.exited:
		return RPCMessage{}, fmt.Errorf("応答待ち中に MCP プロセスが終了しました: %v", bp.exitErr)
	case resp, ok := <-ch:
		if !ok {
			return RPCMessage{}, fmt.Errorf("応答待ち中に MCP プロセスが終了しました: %v", bp.exitErr)
		}
		return resp, nil
	}
}

type MCPUnit struct {
	name       string
	cfg        ServerConfig
	globalIdle string
	logger     *log.Logger

	mu              sync.Mutex
	actMu           sync.Mutex
	status          string // idle | starting | running | stopping | stopped | failed
	proc            *backendProc
	cache           *ServerCache
	reason          string
	startedAt       time.Time
	lastUsedAt      time.Time
	failedAt        time.Time
	exitCode        string
	activeClients   int
	inFlight        int
	totalCalls      int
	idleTimer       *time.Timer
	disconnectTimer *time.Timer
	shuttingDown    bool

	subMu       sync.Mutex
	nextSubID   uint64
	subscribers map[uint64]chan []byte
}

func NewMCPUnit(name string, cfg ServerConfig, globalIdle string, logger *log.Logger) *MCPUnit {
	u := &MCPUnit{
		name:        name,
		cfg:         cfg,
		globalIdle:  globalIdle,
		logger:      logger,
		status:      "idle",
		subscribers: map[uint64]chan []byte{},
	}
	if c, err := LoadServerCache(name); err == nil {
		if c.SpecHash == "" || c.SpecHash == cfg.SpecHash() {
			c.SpecHash = cfg.SpecHash()
			u.cache = c
		}
	}
	return u
}

func (u *MCPUnit) Subscribe() (uint64, <-chan []byte, func()) {
	u.subMu.Lock()
	u.nextSubID++
	id := u.nextSubID
	ch := make(chan []byte, 32)
	u.subscribers[id] = ch
	u.subMu.Unlock()

	u.mu.Lock()
	u.activeClients++
	if u.disconnectTimer != nil {
		u.disconnectTimer.Stop()
		u.disconnectTimer = nil
	}
	u.mu.Unlock()

	cleanup := func() {
		u.subMu.Lock()
		if c, ok := u.subscribers[id]; ok {
			delete(u.subscribers, id)
			close(c)
		}
		u.subMu.Unlock()

		u.mu.Lock()
		if u.activeClients > 0 {
			u.activeClients--
		}
		shouldClose := u.activeClients == 0 && u.inFlight == 0 && u.cfg.ShouldCloseOnDisconnect() &&
			(u.status == "running" || u.status == "starting") && !u.shuttingDown
		if shouldClose {
			if u.disconnectTimer != nil {
				u.disconnectTimer.Stop()
			}
			u.disconnectTimer = time.AfterFunc(DisconnectGrace, func() {
				u.mu.Lock()
				stillIdle := u.activeClients == 0 && u.inFlight == 0 &&
					(u.status == "running" || u.status == "starting") && !u.shuttingDown
				u.mu.Unlock()
				if stillIdle {
					_ = u.Stop("全クライアント切断", true)
				}
			})
		}
		u.mu.Unlock()
	}
	return id, ch, cleanup
}

func (u *MCPUnit) broadcastNotification(line []byte) {
	msgCopy := append([]byte(nil), bytes.TrimRight(line, "\r\n")...)
	u.subMu.Lock()
	defer u.subMu.Unlock()
	for _, ch := range u.subscribers {
		select {
		case ch <- msgCopy:
		default:
		}
	}
}

func (u *MCPUnit) resetIdleTimerLocked() {
	if u.idleTimer != nil {
		u.idleTimer.Stop()
		u.idleTimer = nil
	}
	if u.status != "running" || u.inFlight > 0 || u.shuttingDown {
		return
	}
	timeout := u.cfg.EffectiveIdleTimeout(u.globalIdle)
	if timeout <= 0 {
		return
	}
	u.idleTimer = time.AfterFunc(timeout, func() {
		u.mu.Lock()
		canStop := u.status == "running" && u.inFlight == 0 && !u.shuttingDown &&
			time.Since(u.lastUsedAt) >= timeout
		u.mu.Unlock()
		if canStop {
			_ = u.Stop(fmt.Sprintf("アイドル自動停止 (%s)", timeout), true)
		}
	})
}

func (u *MCPUnit) EnsureRunning(ctx context.Context, reason string, manual bool) (*backendProc, error) {
	u.mu.Lock()
	if u.shuttingDown {
		u.mu.Unlock()
		return nil, errors.New("mcpmd は終了処理中です")
	}
	if u.status == "running" && u.proc != nil {
		bp := u.proc
		u.lastUsedAt = time.Now()
		u.resetIdleTimerLocked()
		u.mu.Unlock()
		return bp, nil
	}
	if u.status == "starting" && u.proc != nil && u.proc.readyRes != nil {
		bp := u.proc
		res := bp.readyRes
		u.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-res.done:
			if res.err != nil {
				return nil, res.err
			}
			return bp, nil
		}
	}
	if !manual && u.status == "failed" && time.Since(u.failedAt) < FailBackoff {
		u.mu.Unlock()
		return nil, fmt.Errorf("%s は直前の起動に失敗しています（mcpm logs %s を確認してください）", u.name, u.name)
	}

	initCtx, cancelInit := context.WithTimeout(context.Background(), ReadyTimeout)
	res := &startResult{done: make(chan struct{})}
	bp := &backendProc{
		exited:     make(chan struct{}),
		pending:    map[uint64]chan RPCMessage{},
		readyRes:   res,
		cancelInit: cancelInit,
		specHash:   u.cfg.SpecHash(),
	}
	u.status = "starting"
	u.proc = bp
	u.reason = reason
	u.startedAt = time.Now()
	u.lastUsedAt = u.startedAt
	u.exitCode = ""
	if u.idleTimer != nil {
		u.idleTimer.Stop()
		u.idleTimer = nil
	}
	u.mu.Unlock()

	if u.logger != nil {
		u.logger.Printf("起動 %s（%s）", u.name, reason)
	}

	go u.spawnAndInit(initCtx, bp, res)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-res.done:
		if res.err != nil {
			return nil, res.err
		}
		return bp, nil
	}
}

func (u *MCPUnit) spawnAndInit(initCtx context.Context, bp *backendProc, res *startResult) {
	defer func() {
		if bp.cancelInit != nil {
			bp.cancelInit()
		}
	}()
	finish := func(err error) {
		res.err = err
		close(res.done)
	}

	u.mu.Lock()
	cfg := u.cfg
	u.mu.Unlock()

	logPath := filepath.Join(LogDir(), u.name+".log")
	lf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		bp.markExited(err)
		u.markFailed(bp, fmt.Sprintf("ログファイルを開けません: %v", err))
		finish(err)
		return
	}
	bp.procMu.Lock()
	bp.logFile = lf
	bp.procMu.Unlock()
	_, _ = fmt.Fprintf(lf, "\n=== mcpm: %s start (%s) ===\n", u.name, time.Now().Format(time.RFC3339))

	cmd := exec.Command(cfg.Command, cfg.Args...)
	if cfg.Cwd != "" {
		cmd.Dir = cfg.Cwd
	}
	env := os.Environ()
	for k, v := range cfg.Env {
		env = append(env, k+"="+v)
	}
	cmd.Env = env
	cmd.Stderr = lf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		bp.markExited(err)
		u.markFailed(bp, err.Error())
		finish(err)
		return
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		bp.markExited(err)
		u.markFailed(bp, err.Error())
		finish(err)
		return
	}

	bp.procMu.Lock()
	if bp.stopping {
		bp.procMu.Unlock()
		_ = stdin.Close()
		_ = stdout.Close()
		stopErr := errors.New("起動中に停止されました")
		bp.markExited(stopErr)
		finish(stopErr)
		return
	}
	if err := cmd.Start(); err != nil {
		bp.procMu.Unlock()
		_ = stdin.Close()
		_ = stdout.Close()
		_, _ = fmt.Fprintf(lf, "[ERROR] failed to start command %q: %v\n", cfg.Command, err)
		bp.markExited(err)
		u.markFailed(bp, err.Error())
		finish(fmt.Errorf("%s の起動に失敗しました: %w", u.name, err))
		return
	}
	bp.cmd = cmd
	bp.stdin = stdin
	bp.pid = cmd.Process.Pid
	pid := bp.pid
	bp.procMu.Unlock()

	go u.readStdout(bp, stdout)
	go func() {
		waitErr := cmd.Wait()
		bp.markExited(waitErr)
		u.onProcessExited(bp, waitErr)
	}()

	initParams, _ := json.Marshal(map[string]any{
		"protocolVersion": DefaultProtocolVer,
		"capabilities":    map[string]any{},
		"clientInfo": map[string]any{
			"name":    "mcpm",
			"version": "0.1.0",
		},
	})
	resp, err := bp.call(initCtx, "initialize", initParams)
	if err != nil {
		killProcessGroup(pid)
		u.markFailed(bp, err.Error())
		finish(fmt.Errorf("%s の initialize に失敗しました: %w", u.name, err))
		return
	}
	if resp.Error != nil {
		killProcessGroup(pid)
		errMsg := fmt.Sprintf("initialize error (%d): %s", resp.Error.Code, resp.Error.Message)
		u.markFailed(bp, errMsg)
		finish(errors.New(errMsg))
		return
	}
	bp.initRaw = resp.Result

	initNotif, _ := json.Marshal(RPCMessage{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
	})
	if err := bp.sendRaw(initNotif); err != nil {
		killProcessGroup(pid)
		u.markFailed(bp, err.Error())
		finish(err)
		return
	}

	u.mu.Lock()
	needCache := u.cache == nil || u.cache.SpecHash != bp.specHash
	u.mu.Unlock()
	if needCache {
		if c, err := u.fetchMetadataFromProc(initCtx, bp); err == nil {
			u.mu.Lock()
			if u.cfg.SpecHash() == c.SpecHash {
				u.cache = c
				u.mu.Unlock()
				_ = SaveServerCache(c)
			} else {
				u.mu.Unlock()
			}
		}
	}

	u.mu.Lock()
	if u.proc == bp && u.status == "starting" && !bp.isStopping() {
		u.status = "running"
		u.lastUsedAt = time.Now()
		u.resetIdleTimerLocked()
		if u.logger != nil {
			u.logger.Printf("準備完了 %s（PID %d, %s）", u.name, pid, time.Since(u.startedAt).Round(10*time.Millisecond))
		}
	}
	u.mu.Unlock()

	if bp.isStopping() {
		finish(errors.New("起動中に停止されました"))
		return
	}
	finish(nil)
}

func (u *MCPUnit) readStdout(bp *backendProc, r io.Reader) {
	br := bufio.NewReaderSize(r, 64*1024)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				u.handleBackendLine(bp, trimmed)
			}
		}
		if err != nil {
			return
		}
	}
}

func (u *MCPUnit) handleBackendLine(bp *backendProc, line []byte) {
	var msg RPCMessage
	if err := json.Unmarshal(line, &msg); err != nil {
		bp.procMu.Lock()
		lf := bp.logFile
		bp.procMu.Unlock()
		if lf != nil {
			_, _ = fmt.Fprintf(lf, "[stdout] %s\n", string(line))
		}
		return
	}

	if msg.IsResponse() {
		var seqID uint64
		if err := json.Unmarshal(msg.ID, &seqID); err == nil {
			bp.pendMu.Lock()
			ch, ok := bp.pending[seqID]
			if ok {
				delete(bp.pending, seqID)
			}
			bp.pendMu.Unlock()
			if ok {
				ch <- msg
			}
		}
		return
	}

	if msg.IsNotification() {
		u.broadcastNotification(line)
		if strings.HasSuffix(msg.Method, "/list_changed") {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				if c, err := u.fetchMetadataFromProc(ctx, bp); err == nil {
					u.mu.Lock()
					if u.cfg.SpecHash() == c.SpecHash {
						u.cache = c
						u.mu.Unlock()
						_ = SaveServerCache(c)
					} else {
						u.mu.Unlock()
					}
				}
			}()
		}
		return
	}

	if msg.IsRequest() {
		switch msg.Method {
		case "ping":
			if out, err := MakeRPCResult(msg.ID, map[string]any{}); err == nil {
				_ = bp.sendRaw(out)
			}
		case "roots/list":
			if out, err := MakeRPCResult(msg.ID, map[string]any{"roots": []any{}}); err == nil {
				_ = bp.sendRaw(out)
			}
		default:
			if out, err := MakeRPCError(msg.ID, -32601, "Method not supported by mcpm proxy"); err == nil {
				_ = bp.sendRaw(out)
			}
		}
	}
}

func (u *MCPUnit) fetchMetadataFromProc(ctx context.Context, bp *backendProc) (*ServerCache, error) {
	specHash := bp.specHash
	if specHash == "" {
		u.mu.Lock()
		specHash = u.cfg.SpecHash()
		u.mu.Unlock()
	}
	cache := &ServerCache{
		ServerName:       u.name,
		SpecHash:         specHash,
		UpdatedAt:        time.Now(),
		InitializeResult: bp.initRaw,
		Tools:            []json.RawMessage{},
	}

	var cursor string
	for range 20 {
		var params json.RawMessage
		if cursor != "" {
			params, _ = json.Marshal(map[string]string{"cursor": cursor})
		} else {
			params = json.RawMessage(`{}`)
		}
		resp, err := bp.call(ctx, "tools/list", params)
		if err != nil || resp.Error != nil {
			break
		}
		var parsed struct {
			Tools      []json.RawMessage `json:"tools"`
			NextCursor string            `json:"nextCursor"`
		}
		if err := json.Unmarshal(resp.Result, &parsed); err != nil {
			break
		}
		cache.Tools = append(cache.Tools, parsed.Tools...)
		if parsed.NextCursor == "" {
			break
		}
		cursor = parsed.NextCursor
	}

	var initParsed struct {
		Capabilities map[string]json.RawMessage `json:"capabilities"`
	}
	_ = json.Unmarshal(bp.initRaw, &initParsed)

	if _, hasRes := initParsed.Capabilities["resources"]; hasRes {
		if resp, err := bp.call(ctx, "resources/list", json.RawMessage(`{}`)); err == nil && resp.Error == nil {
			var parsed struct {
				Resources []json.RawMessage `json:"resources"`
			}
			if json.Unmarshal(resp.Result, &parsed) == nil {
				cache.Resources = parsed.Resources
			}
		}
		if resp, err := bp.call(ctx, "resources/templates/list", json.RawMessage(`{}`)); err == nil && resp.Error == nil {
			var parsed struct {
				ResourceTemplates []json.RawMessage `json:"resourceTemplates"`
			}
			if json.Unmarshal(resp.Result, &parsed) == nil {
				cache.ResourceTemplates = parsed.ResourceTemplates
			}
		}
	}

	if _, hasPrompts := initParsed.Capabilities["prompts"]; hasPrompts {
		if resp, err := bp.call(ctx, "prompts/list", json.RawMessage(`{}`)); err == nil && resp.Error == nil {
			var parsed struct {
				Prompts []json.RawMessage `json:"prompts"`
			}
			if json.Unmarshal(resp.Result, &parsed) == nil {
				cache.Prompts = parsed.Prompts
			}
		}
	}

	return cache, nil
}

func (u *MCPUnit) EnsureCache(ctx context.Context, forceRefresh bool) (*ServerCache, error) {
	u.mu.Lock()
	wantSpec := u.cfg.SpecHash()
	if !forceRefresh && u.cache != nil && u.cache.SpecHash == wantSpec {
		c := u.cache
		u.mu.Unlock()
		return c, nil
	}
	wasRunning := u.status == "running" || u.status == "starting"
	u.mu.Unlock()

	bp, err := u.EnsureRunning(ctx, "スキーマ取得 (cache)", forceRefresh)
	if err != nil {
		return nil, err
	}

	u.mu.Lock()
	wantSpec = u.cfg.SpecHash()
	missingCache := u.cache == nil || u.cache.SpecHash != wantSpec
	u.mu.Unlock()

	if forceRefresh || missingCache {
		c, err := u.fetchMetadataFromProc(ctx, bp)
		if err != nil {
			return nil, err
		}
		u.mu.Lock()
		u.cache = c
		u.mu.Unlock()
		_ = SaveServerCache(c)
	}

	u.mu.Lock()
	c := u.cache
	shouldKill := !wasRunning && u.inFlight == 0
	u.mu.Unlock()

	if shouldKill {
		_ = u.Stop("スキーマ取得完了 (自動kill)", true)
	}

	if c == nil {
		return nil, fmt.Errorf("%s のスキーマキャッシュを取得できませんでした", u.name)
	}
	return c, nil
}

func (u *MCPUnit) HandleMessage(ctx context.Context, rawLine []byte) ([]byte, error) {
	var msg RPCMessage
	if err := json.Unmarshal(rawLine, &msg); err != nil {
		return MakeRPCError(json.RawMessage("null"), -32700, "Parse error")
	}

	if msg.IsNotification() {
		if msg.Method == "notifications/initialized" {
			return nil, nil
		}
		u.mu.Lock()
		bp := u.proc
		running := u.status == "running" && bp != nil
		u.mu.Unlock()
		if running {
			_ = bp.sendRaw(rawLine)
		}
		return nil, nil
	}

	if !msg.IsRequest() {
		return nil, nil
	}

	if IsLightweightMethod(msg.Method) {
		return u.handleLightweightRequest(ctx, msg)
	}

	return u.handleHeavyRequest(ctx, msg)
}

func (u *MCPUnit) handleLightweightRequest(ctx context.Context, msg RPCMessage) ([]byte, error) {
	switch msg.Method {
	case "ping", "logging/setLevel":
		return MakeRPCResult(msg.ID, map[string]any{})
	}

	cache, err := u.EnsureCache(ctx, false)
	if err != nil {
		return MakeRPCError(msg.ID, -32603, err.Error())
	}

	switch msg.Method {
	case "initialize":
		var clientParams struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(msg.Params, &clientParams)

		var initObj map[string]any
		if err := json.Unmarshal(cache.InitializeResult, &initObj); err != nil || initObj == nil {
			initObj = map[string]any{
				"protocolVersion": DefaultProtocolVer,
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo":      map[string]any{"name": u.name, "version": "0.1.0"},
			}
		}
		if clientParams.ProtocolVersion != "" {
			initObj["protocolVersion"] = clientParams.ProtocolVersion
		}
		return MakeRPCResult(msg.ID, initObj)

	case "tools/list":
		tools := cache.Tools
		if tools == nil {
			tools = []json.RawMessage{}
		}
		return MakeRPCResult(msg.ID, map[string]any{"tools": tools})

	case "resources/list":
		res := cache.Resources
		if res == nil {
			res = []json.RawMessage{}
		}
		return MakeRPCResult(msg.ID, map[string]any{"resources": res})

	case "resources/templates/list":
		tpls := cache.ResourceTemplates
		if tpls == nil {
			tpls = []json.RawMessage{}
		}
		return MakeRPCResult(msg.ID, map[string]any{"resourceTemplates": tpls})

	case "prompts/list":
		prompts := cache.Prompts
		if prompts == nil {
			prompts = []json.RawMessage{}
		}
		return MakeRPCResult(msg.ID, map[string]any{"prompts": prompts})

	default:
		return MakeRPCResult(msg.ID, map[string]any{})
	}
}

func (u *MCPUnit) handleHeavyRequest(ctx context.Context, msg RPCMessage) ([]byte, error) {
	reason := msg.Method
	if msg.Method == "tools/call" {
		var p struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(msg.Params, &p) == nil && p.Name != "" {
			reason = "tools/call: " + p.Name
		}
	}

	u.mu.Lock()
	u.inFlight++
	u.totalCalls++
	if u.idleTimer != nil {
		u.idleTimer.Stop()
		u.idleTimer = nil
	}
	if u.disconnectTimer != nil {
		u.disconnectTimer.Stop()
		u.disconnectTimer = nil
	}
	u.mu.Unlock()

	defer func() {
		u.mu.Lock()
		if u.inFlight > 0 {
			u.inFlight--
		}
		u.lastUsedAt = time.Now()
		u.resetIdleTimerLocked()
		if u.activeClients == 0 && u.inFlight == 0 && u.cfg.ShouldCloseOnDisconnect() && u.status == "running" && !u.shuttingDown {
			if u.disconnectTimer != nil {
				u.disconnectTimer.Stop()
			}
			u.disconnectTimer = time.AfterFunc(DisconnectGrace, func() {
				u.mu.Lock()
				stillIdle := u.activeClients == 0 && u.inFlight == 0 &&
					(u.status == "running" || u.status == "starting") && !u.shuttingDown
				u.mu.Unlock()
				if stillIdle {
					_ = u.Stop("全クライアント切断", true)
				}
			})
		}
		u.mu.Unlock()
	}()

	bp, err := u.EnsureRunning(ctx, reason, false)
	if err != nil {
		return MakeRPCError(msg.ID, -32603, err.Error())
	}

	u.mu.Lock()
	u.reason = reason
	u.mu.Unlock()

	resp, err := bp.call(ctx, msg.Method, msg.Params)
	if err != nil {
		return MakeRPCError(msg.ID, -32603, err.Error())
	}

	resp.JSONRPC = "2.0"
	resp.ID = msg.ID
	return json.Marshal(resp)
}

func (u *MCPUnit) onProcessExited(bp *backendProc, waitErr error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.proc != bp {
		return
	}
	u.proc = nil
	if u.idleTimer != nil {
		u.idleTimer.Stop()
		u.idleTimer = nil
	}
	if u.status == "stopping" || u.status == "stopped" || u.status == "idle" {
		return
	}
	code := "exit 0"
	if waitErr != nil {
		code = waitErr.Error()
	}
	u.status = "failed"
	u.failedAt = time.Now()
	u.exitCode = code
	if u.logger != nil {
		u.logger.Printf("失敗 %s（%s）", u.name, code)
	}
}

func (u *MCPUnit) markFailed(bp *backendProc, reason string) {
	if bp.isStopping() {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.status == "stopping" || u.status == "stopped" || u.status == "idle" {
		if u.proc == bp {
			u.proc = nil
		}
		return
	}
	if u.proc == bp {
		u.proc = nil
	}
	u.status = "failed"
	u.failedAt = time.Now()
	u.exitCode = reason
	if u.logger != nil {
		u.logger.Printf("失敗 %s（%s）", u.name, reason)
	}
}

func (u *MCPUnit) Stop(reason string, toIdle bool) error {
	u.actMu.Lock()
	defer u.actMu.Unlock()

	u.mu.Lock()
	if u.idleTimer != nil {
		u.idleTimer.Stop()
		u.idleTimer = nil
	}
	if u.disconnectTimer != nil {
		u.disconnectTimer.Stop()
		u.disconnectTimer = nil
	}
	bp := u.proc
	if bp == nil || (u.status != "running" && u.status != "starting") {
		if toIdle {
			u.status = "idle"
		} else if u.status != "idle" {
			u.status = "stopped"
		}
		if reason != "" {
			u.reason = reason
		}
		u.mu.Unlock()
		return nil
	}
	u.status = "stopping"
	u.mu.Unlock()

	stdin, pid := bp.requestStop()
	if stdin != nil {
		_ = stdin.Close()
	}
	if pid > 0 {
		_ = syscall.Kill(-pid, syscall.SIGTERM)
	}

	select {
	case <-bp.exited:
	case <-time.After(StopGrace):
		bp.procMu.Lock()
		killPid := bp.pid
		bp.procMu.Unlock()
		if killPid > 0 {
			_ = syscall.Kill(-killPid, syscall.SIGKILL)
		}
		<-bp.exited
	}

	u.mu.Lock()
	if u.proc == bp {
		u.proc = nil
	}
	if toIdle {
		u.status = "idle"
	} else {
		u.status = "stopped"
	}
	if reason != "" {
		u.reason = reason
	}
	u.startedAt = time.Time{}
	u.mu.Unlock()

	if u.logger != nil {
		u.logger.Printf("停止 %s（%s）", u.name, reason)
	}
	return nil
}

func (u *MCPUnit) Restart(ctx context.Context, reason string) error {
	if err := u.Stop("再起動のため停止", true); err != nil {
		return err
	}
	_, err := u.EnsureRunning(ctx, reason, true)
	return err
}

func killProcessGroup(pid int) {
	if pid <= 0 {
		return
	}
	_ = syscall.Kill(-pid, syscall.SIGTERM)
	time.AfterFunc(StopGrace, func() {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	})
}

type UnitStatus struct {
	Name          string `json:"name"`
	Port          int    `json:"port,omitempty"`
	Status        string `json:"status"`
	Pid           int    `json:"pid,omitempty"`
	StartedAt     string `json:"startedAt,omitempty"`
	LastUsedAt    string `json:"lastUsedAt,omitempty"`
	ActiveClients int    `json:"activeClients"`
	InFlight      int    `json:"inFlight"`
	TotalCalls    int    `json:"totalCalls"`
	CachedTools   int    `json:"cachedTools"`
	Cached        bool   `json:"cached"`
	IdleTimeout   string `json:"idleTimeout,omitempty"`
	Reason        string `json:"reason,omitempty"`
	Command       string `json:"command"`
}

func (u *MCPUnit) Snapshot() UnitStatus {
	u.mu.Lock()
	defer u.mu.Unlock()

	pid := 0
	if u.proc != nil {
		u.proc.procMu.Lock()
		pid = u.proc.pid
		u.proc.procMu.Unlock()
	}
	started := ""
	if !u.startedAt.IsZero() {
		started = u.startedAt.Format(time.RFC3339)
	}
	lastUsed := ""
	if !u.lastUsedAt.IsZero() {
		lastUsed = u.lastUsedAt.Format(time.RFC3339)
	}
	toolsCount := 0
	cached := false
	if u.cache != nil {
		cached = true
		toolsCount = len(u.cache.Tools)
	}
	idleStr := ""
	if d := u.cfg.EffectiveIdleTimeout(u.globalIdle); d > 0 {
		idleStr = d.String()
	}
	cmdStr := u.cfg.Command
	if len(u.cfg.Args) > 0 {
		cmdStr += " " + strings.Join(u.cfg.Args, " ")
	}
	return UnitStatus{
		Name:          u.name,
		Port:          u.cfg.Port,
		Status:        u.status,
		Pid:           pid,
		StartedAt:     started,
		LastUsedAt:    lastUsed,
		ActiveClients: u.activeClients,
		InFlight:      u.inFlight,
		TotalCalls:    u.totalCalls,
		CachedTools:   toolsCount,
		Cached:        cached,
		IdleTimeout:   idleStr,
		Reason:        u.reason,
		Command:       cmdStr,
	}
}
