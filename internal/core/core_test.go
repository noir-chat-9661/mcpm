package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestHelperMCPServer(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_MCP") != "1" {
		return
	}
	pid := os.Getpid()
	dec := json.NewDecoder(os.Stdin)

	for {
		var msg RPCMessage
		if err := dec.Decode(&msg); err != nil {
			os.Exit(0)
		}
		if msg.IsNotification() {
			continue
		}
		switch msg.Method {
		case "initialize":
			if dStr := os.Getenv("HELPER_INIT_DELAY_MS"); dStr != "" {
				if ms, err := strconv.Atoi(dStr); err == nil && ms > 0 {
					time.Sleep(time.Duration(ms) * time.Millisecond)
				}
			}
			res, _ := MakeRPCResult(msg.ID, map[string]any{
				"protocolVersion": "2024-11-05",
				"capabilities": map[string]any{
					"tools": map[string]any{},
				},
				"serverInfo": map[string]any{
					"name":    "mock-mcp",
					"version": "1.0.0",
				},
			})
			_, _ = os.Stdout.Write(append(res, '\n'))
		case "tools/list":
			toolName := os.Getenv("HELPER_TOOL_NAME")
			if toolName == "" {
				toolName = "echo_pid"
			}
			res, _ := MakeRPCResult(msg.ID, map[string]any{
				"tools": []map[string]any{
					{
						"name":        toolName,
						"description": "Returns backend PID and echoed text",
						"inputSchema": map[string]any{"type": "object"},
					},
				},
			})
			_, _ = os.Stdout.Write(append(res, '\n'))
		case "tools/call":
			var p struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			}
			_ = json.Unmarshal(msg.Params, &p)
			if delayMs, ok := p.Arguments["delayMs"].(float64); ok && delayMs > 0 {
				time.Sleep(time.Duration(delayMs) * time.Millisecond)
			}
			res, _ := MakeRPCResult(msg.ID, map[string]any{
				"pid":  pid,
				"echo": p.Arguments["text"],
			})
			_, _ = os.Stdout.Write(append(res, '\n'))
		}
	}
}

func newTestServerConfig(t *testing.T, idleTimeout string) ServerConfig {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("Executable: %v", err)
	}
	return ServerConfig{
		Command:     exe,
		Args:        []string{"-test.run=^TestHelperMCPServer$"},
		Env:         map[string]string{"GO_WANT_HELPER_MCP": "1"},
		IdleTimeout: idleTimeout,
	}
}

func TestLazyCacheAndCoexistenceAndClose(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("MCPM_HOME", tmp)
	if err := EnsureHomeDir(); err != nil {
		t.Fatal(err)
	}

	logger := log.New(io.Discard, "", 0)
	sc := newTestServerConfig(t, "300ms")
	u := NewMCPUnit("mock", sc, "300ms", logger)
	ctx := context.Background()

	// 1. 初回 initialize: キャッシュ取得後に即座に自動 kill されること
	initReq := []byte(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`)
	initRespBytes, err := u.HandleMessage(ctx, initReq)
	if err != nil {
		t.Fatalf("initialize error: %v", err)
	}
	if !bytes.Contains(initRespBytes, []byte(`"mock-mcp"`)) {
		t.Fatalf("unexpected initialize response: %s", string(initRespBytes))
	}

	snap := u.Snapshot()
	if snap.Status != "idle" || snap.Pid != 0 {
		t.Fatalf("初回スキーマ取得後は自動 kill され idle (PID 0) になるべきですが status=%s pid=%d でした", snap.Status, snap.Pid)
	}
	if !snap.Cached || snap.CachedTools != 1 {
		t.Fatalf("キャッシュされたツール数が 1 であるべきですが cached=%v tools=%d でした", snap.Cached, snap.CachedTools)
	}

	// 2. 2 回目以降の tools/list: プロセスを起動せずキャッシュから即答すること
	listReq := []byte(`{"jsonrpc":"2.0","id":"list-1","method":"tools/list","params":{}}`)
	listRespBytes, err := u.HandleMessage(ctx, listReq)
	if err != nil {
		t.Fatalf("tools/list error: %v", err)
	}
	if !bytes.Contains(listRespBytes, []byte(`"echo_pid"`)) {
		t.Fatalf("unexpected tools/list response: %s", string(listRespBytes))
	}
	snap = u.Snapshot()
	if snap.Status != "idle" || snap.Pid != 0 {
		t.Fatalf("tools/list ではプロセスを起動しないはずですが status=%s pid=%d でした", snap.Status, snap.Pid)
	}

	// 3. 複数クライアントからの同時 tools/call: 1 つのプロセスだけを起動し共有すること
	_, _, unsub1 := u.Subscribe()
	_, _, unsub2 := u.Subscribe()

	var wg sync.WaitGroup
	pids := make([]int, 2)
	echoes := make([]string, 2)
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			req := fmt.Sprintf(`{"jsonrpc":"2.0","id":"call-%d","method":"tools/call","params":{"name":"echo_pid","arguments":{"text":"hello-%d","delayMs":60}}}`, idx, idx)
			respBytes, err := u.HandleMessage(ctx, []byte(req))
			if err != nil {
				t.Errorf("call %d error: %v", idx, err)
				return
			}
			var rpcResp struct {
				ID     string `json:"id"`
				Result struct {
					Pid  int    `json:"pid"`
					Echo string `json:"echo"`
				} `json:"result"`
			}
			if err := json.Unmarshal(respBytes, &rpcResp); err != nil {
				t.Errorf("unmarshal call %d: %v (%s)", idx, err, string(respBytes))
				return
			}
			pids[idx] = rpcResp.Result.Pid
			echoes[idx] = rpcResp.Result.Echo
		}(i)
	}
	wg.Wait()

	if pids[0] == 0 || pids[0] != pids[1] {
		t.Fatalf("2 つの同時呼び出しは同じ MCP プロセスを共有すべきですが pid0=%d, pid1=%d でした", pids[0], pids[1])
	}
	if echoes[0] != "hello-0" || echoes[1] != "hello-1" {
		t.Fatalf("レスポンスの多重化ルーティングが不正です: %v", echoes)
	}

	// 4. 手動 close: クライアント接続を維持したままバックエンドプロセスを kill できること
	if err := u.Stop("手動 close", true); err != nil {
		t.Fatalf("stop error: %v", err)
	}
	snap = u.Snapshot()
	if snap.Status != "idle" || snap.Pid != 0 {
		t.Fatalf("close 後は idle (PID 0) のはずですが status=%s pid=%d でした", snap.Status, snap.Pid)
	}

	// 5. close 後に再度 tools/call が来たら透過的に再起動すること
	callAgain := []byte(`{"jsonrpc":"2.0","id":99,"method":"tools/call","params":{"name":"echo_pid","arguments":{"text":"again"}}}`)
	respAgain, err := u.HandleMessage(ctx, callAgain)
	if err != nil {
		t.Fatalf("callAgain error: %v", err)
	}
	var parsedAgain struct {
		Result struct {
			Pid int `json:"pid"`
		} `json:"result"`
	}
	_ = json.Unmarshal(respAgain, &parsedAgain)
	if parsedAgain.Result.Pid == 0 || parsedAgain.Result.Pid == pids[0] {
		t.Fatalf("再起動後は新しい PID で動くはずですが old=%d new=%d でした", pids[0], parsedAgain.Result.Pid)
	}

	// 6. アイドルタイムアウト（300ms）で自動 close (kill) されること
	time.Sleep(450 * time.Millisecond)
	snap = u.Snapshot()
	if snap.Status != "idle" || snap.Pid != 0 {
		t.Fatalf("アイドルタイムアウト後は自動 kill されるべきですが status=%s pid=%d でした", snap.Status, snap.Pid)
	}

	unsub1()
	unsub2()
}

func TestHTTPPortListenerLazyProxy(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("MCPM_HOME", tmp)
	if err := EnsureHomeDir(); err != nil {
		t.Fatal(err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	sc := newTestServerConfig(t, "5m")
	sc.Port = port
	cfg := &Config{
		GatewayPort: 0,
		Servers: map[string]ServerConfig{
			"http-mock": sc,
		},
	}

	logger := log.New(io.Discard, "", 0)
	d := NewDaemon(cfg, logger)
	if err := d.BindServerPorts(); err != nil {
		t.Fatal(err)
	}
	defer d.CloseListeners()
	defer func() { _ = d.ResolveUnit("http-mock").Stop("test end", true) }()

	baseURL := "http://127.0.0.1:" + strconv.Itoa(port) + "/mcp"

	res, err := http.Post(baseURL, "application/json", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if !bytes.Contains(body, []byte(`"echo_pid"`)) {
		t.Fatalf("unexpected HTTP tools/list response: %s", string(body))
	}
	if st := d.ResolveUnit("http-mock").Snapshot(); st.Pid != 0 {
		t.Fatalf("tools/list 後はプロセスが停止しているべきですが PID=%d でした", st.Pid)
	}

	res2, err := http.Post(baseURL, "application/json", bytes.NewBufferString(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo_pid","arguments":{"text":"via-http"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	body2, _ := io.ReadAll(res2.Body)
	_ = res2.Body.Close()
	if !bytes.Contains(body2, []byte(`"via-http"`)) {
		t.Fatalf("unexpected HTTP tools/call response: %s", string(body2))
	}
	if st := d.ResolveUnit("http-mock").Snapshot(); st.Status != "running" || st.Pid == 0 {
		t.Fatalf("tools/call 後は running のはずですが status=%s pid=%d でした", st.Status, st.Pid)
	}
}

func TestUDSConnectAndAutoCloseOnDisconnect(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("MCPM_HOME", tmp)
	if err := EnsureHomeDir(); err != nil {
		t.Fatal(err)
	}

	sc := newTestServerConfig(t, "5m")
	cfg := &Config{
		GatewayPort: 0,
		Servers: map[string]ServerConfig{
			"uds-mock": sc,
		},
	}
	logger := log.New(io.Discard, "", 0)
	d := NewDaemon(cfg, logger)

	sock := SocketPath()
	_ = os.Remove(sock)
	ctl, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(sock)
	defer ctl.Close()
	ctlSrv := &http.Server{Handler: d.ControlHandler()}
	go ctlSrv.Serve(ctl)

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(conn, "POST /connect?name=uds-mock HTTP/1.1\r\nHost: pm\r\n\r\n")
	dec := json.NewDecoder(conn)

	var hdrBuf [1]byte
	var tail []byte
	for {
		if _, err := conn.Read(hdrBuf[:]); err != nil {
			t.Fatal(err)
		}
		tail = append(tail, hdrBuf[0])
		if bytes.HasSuffix(tail, []byte("\r\n\r\n")) {
			break
		}
	}

	_, _ = io.WriteString(conn, `{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"echo_pid","arguments":{"text":"uds"}}}`+"\n")
	var resp RPCMessage
	if err := dec.Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(resp.Result, []byte(`"uds"`)) {
		t.Fatalf("unexpected UDS response: %s", string(resp.Result))
	}
	if st := d.ResolveUnit("uds-mock").Snapshot(); st.Status != "running" || st.ActiveClients != 1 {
		t.Fatalf("接続中は running (clients=1) のはずですが status=%s clients=%d でした", st.Status, st.ActiveClients)
	}

	_ = conn.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if st := d.ResolveUnit("uds-mock").Snapshot(); st.Status == "idle" && st.Pid == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	st := d.ResolveUnit("uds-mock").Snapshot()
	t.Fatalf("全クライアント切断後に自動 close されるべきですが status=%s pid=%d clients=%d でした", st.Status, st.Pid, st.ActiveClients)
}

func TestStopDuringStartingNoPanic(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("MCPM_HOME", tmp)
	if err := EnsureHomeDir(); err != nil {
		t.Fatal(err)
	}

	logger := log.New(io.Discard, "", 0)
	sc := newTestServerConfig(t, "5m")
	sc.Env["HELPER_INIT_DELAY_MS"] = "250"
	u := NewMCPUnit("race-stop", sc, "5m", logger)

	// 1. 子プロセス準備前〜initialize 応答待ちの各タイミングで Stop を競合させても panic しないこと
	for range 10 {
		done := make(chan struct{})
		go func() {
			defer close(done)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = u.EnsureRunning(ctx, "race start", true)
		}()

		deadline := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(deadline) {
			if u.Snapshot().Status == "starting" {
				break
			}
			time.Sleep(100 * time.Microsecond)
		}

		if err := u.Stop("起動直後の手動 close", true); err != nil {
			t.Fatalf("Stop during starting failed: %v", err)
		}
		<-done

		st := u.Snapshot()
		if st.Status != "idle" || st.Pid != 0 {
			t.Fatalf("Stop 後は idle (PID 0) であるべきですが status=%s pid=%d でした", st.Status, st.Pid)
		}
	}
}

func TestReloadPortRebindAndSchemaCacheInvalidation(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("MCPM_HOME", tmp)
	if err := EnsureHomeDir(); err != nil {
		t.Fatal(err)
	}

	allocPort := func() int {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		p := l.Addr().(*net.TCPAddr).Port
		_ = l.Close()
		return p
	}
	port1 := allocPort()
	port2 := allocPort()

	sc := newTestServerConfig(t, "5m")
	sc.Port = port1
	sc.Env["HELPER_TOOL_NAME"] = "tool_v1"

	cfg := &Config{
		GatewayPort: 0,
		Servers: map[string]ServerConfig{
			"rebind-mock": sc,
		},
	}
	if err := SaveConfig(cfg); err != nil {
		t.Fatal(err)
	}

	logger := log.New(io.Discard, "", 0)
	d := NewDaemon(cfg, logger)
	if err := d.BindServerPorts(); err != nil {
		t.Fatal(err)
	}
	defer d.CloseListeners()
	defer func() { _ = d.ResolveUnit("rebind-mock").Stop("test end", true) }()

	url1 := "http://127.0.0.1:" + strconv.Itoa(port1) + "/mcp"
	url2 := "http://127.0.0.1:" + strconv.Itoa(port2) + "/mcp"

	res1, err := http.Post(url1, "application/json", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	if err != nil {
		t.Fatalf("initial port1 request failed: %v", err)
	}
	body1, _ := io.ReadAll(res1.Body)
	_ = res1.Body.Close()
	if !bytes.Contains(body1, []byte(`"tool_v1"`)) {
		t.Fatalf("expected tool_v1 on port1, got: %s", string(body1))
	}

	// ポート変更 (port1 -> port2) かつコマンド環境変更 (tool_v1 -> tool_v2) して reload
	sc2 := newTestServerConfig(t, "5m")
	sc2.Port = port2
	sc2.Env["HELPER_TOOL_NAME"] = "tool_v2"
	cfg.Servers["rebind-mock"] = sc2
	if err := SaveConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := d.Act("reload", "", ""); err != nil {
		t.Fatalf("reload failed: %v", err)
	}

	// 旧ポート (port1) は閉じられていること
	if _, err := http.Post(url1, "application/json", bytes.NewBufferString(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)); err == nil {
		t.Fatalf("old port %d should be closed after reload, but request succeeded", port1)
	}

	// 新ポート (port2) で待ち受けられ、かつ新しいスキーマ (tool_v2) が返ること
	res2, err := http.Post(url2, "application/json", bytes.NewBufferString(`{"jsonrpc":"2.0","id":3,"method":"tools/list"}`))
	if err != nil {
		t.Fatalf("new port2 request failed: %v", err)
	}
	body2, _ := io.ReadAll(res2.Body)
	_ = res2.Body.Close()
	if !bytes.Contains(body2, []byte(`"tool_v2"`)) || bytes.Contains(body2, []byte(`"tool_v1"`)) {
		t.Fatalf("expected updated schema tool_v2 on port2, got: %s", string(body2))
	}
}
