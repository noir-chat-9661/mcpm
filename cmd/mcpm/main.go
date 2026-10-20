// mcpm: MCP Compress / Process Manager の CLI・TUI フロントエンド（叩き台）。
//
// mcpm 自体は常駐プロセスではなく、別プロセスとして動作する実プロセス `mcpmd` に対して
// UDS 越しに操作・状態表示（TUI / CLI）・stdio ブリッジ接続を行うクライアント。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/noir-chat-9661/mcpm/internal/core"
)

type cliArgs struct {
	command    string
	positional []string
	rawAfterDD []string
	flags      map[string]string
	multiFlags map[string][]string
}

func parseArgs(argv []string) cliArgs {
	a := cliArgs{
		flags:      map[string]string{},
		multiFlags: map[string][]string{},
	}
	boolFlags := map[string]bool{
		"f":       true,
		"no-tui":  true,
		"daemon":  true,
		"http":    true,
		"no-disc": true,
		"help":    true,
		"h":       true,
	}
	for i := 0; i < len(argv); i++ {
		s := argv[i]
		if s == "--" {
			a.rawAfterDD = append(a.rawAfterDD, argv[i+1:]...)
			break
		}
		if !strings.HasPrefix(s, "-") {
			if a.command == "" {
				a.command = s
			} else {
				a.positional = append(a.positional, s)
			}
			continue
		}
		key := strings.TrimLeft(s, "-")
		if k, v, ok := strings.Cut(key, "="); ok {
			a.flags[k] = v
			a.multiFlags[k] = append(a.multiFlags[k], v)
		} else if boolFlags[key] || i+1 >= len(argv) {
			a.flags[key] = "true"
		} else {
			val := argv[i+1]
			a.flags[key] = val
			a.multiFlags[key] = append(a.multiFlags[key], val)
			i++
		}
	}
	return a
}

func main() {
	if err := run(parseArgs(os.Args[1:])); err != nil {
		fmt.Fprintln(os.Stderr, "mcpm:", err)
		os.Exit(1)
	}
}

func run(a cliArgs) error {
	if a.flags["help"] == "true" || a.flags["h"] == "true" || a.command == "help" {
		printUsage()
		return nil
	}

	switch a.command {
	case "", "tui":
		conn, err := ensureDaemonRunning(nil)
		if err != nil {
			return err
		}
		_ = conn.Close()
		if isTerminal(os.Stdout) && a.flags["no-tui"] != "true" {
			return runTUI()
		}
		return runPS(a.flags["format"] == "json")

	case "start":
		return cmdStart(a)

	case "ps":
		return runPS(a.flags["format"] == "json")

	case "connect":
		if len(a.positional) == 0 {
			return errors.New("使い方: mcpm connect <MCPサーバー名>")
		}
		return runConnect(a.positional[0])

	case "up", "start-unit", "stop", "close", "restart":
		if len(a.positional) == 0 {
			return fmt.Errorf("使い方: mcpm %s <MCPサーバー名 | ポート> [--reason <理由>]", a.command)
		}
		action := a.command
		if action == "up" || action == "start-unit" {
			action = "start"
		}
		conn, err := ensureDaemonRunning(nil)
		if err != nil {
			return err
		}
		_ = conn.Close()

		var res core.PSResponse
		body := core.ControlRequest{Unit: a.positional[0], Reason: a.flags["reason"]}
		if err := call("POST", "/"+action, body, &res); err != nil {
			return err
		}
		return printUnits(res, a.flags["format"] == "json")

	case "refresh":
		target := "all"
		if len(a.positional) > 0 {
			target = a.positional[0]
		}
		conn, err := ensureDaemonRunning(nil)
		if err != nil {
			return err
		}
		_ = conn.Close()

		var res core.PSResponse
		if err := call("POST", "/refresh", core.ControlRequest{Unit: target, Reason: "手動スキーマ更新"}, &res); err != nil {
			return err
		}
		return printUnits(res, a.flags["format"] == "json")

	case "reload":
		var res core.PSResponse
		if err := call("POST", "/reload", core.ControlRequest{}, &res); err != nil {
			return err
		}
		return printUnits(res, a.flags["format"] == "json")

	case "add":
		return cmdAdd(a)

	case "remove", "rm":
		return cmdRemove(a)

	case "import":
		return cmdImport(a)

	case "client-config":
		return cmdClientConfig(a)

	case "logs":
		return cmdLogs(a)

	case "shutdown":
		var res struct{ OK bool }
		if err := call("POST", "/shutdown", nil, &res); err != nil {
			return err
		}
		fmt.Println("mcpm: 実プロセス (mcpmd) と全 MCP サーバーを停止しました")
		return nil

	default:
		printUsage()
		return fmt.Errorf("不明なコマンド: %s", a.command)
	}
}

func cmdStart(a cliArgs) error {
	cfg, err := core.LoadConfig()
	if err != nil {
		return err
	}
	var warm []string
	for _, q := range strings.Split(a.flags["warm"], ",") {
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}
		name := cfg.Resolve(q)
		if name == "" {
			return fmt.Errorf("--warm: %q に該当する MCP サーバーが見つかりません", q)
		}
		warm = append(warm, name)
	}

	conn, err := ensureDaemonRunning(warm)
	if err != nil {
		return err
	}
	_ = conn.Close()

	if a.flags["daemon"] == "true" || a.flags["no-tui"] == "true" || !isTerminal(os.Stdout) {
		var res core.PSResponse
		if err := call("GET", "/ps", nil, &res); err == nil {
			fmt.Printf("mcpm: 実プロセス mcpmd (PID %d) が稼働中です\n", res.DaemonPid)
		}
		return nil
	}
	return runTUI()
}

func runPS(asJSON bool) error {
	var res core.PSResponse
	if err := call("GET", "/ps", nil, &res); err != nil {
		cfg, cErr := core.LoadConfig()
		if cErr != nil {
			return err
		}
		res = core.PSResponse{
			ConfigPath:  core.ConfigPath(),
			GatewayPort: cfg.GatewayPort,
		}
		for _, name := range cfg.SortedServerNames() {
			sc := cfg.Servers[name]
			u := core.NewMCPUnit(name, sc, cfg.IdleTimeout, nil)
			st := u.Snapshot()
			st.Reason = "mcpmd 未起動"
			res.Units = append(res.Units, st)
		}
	}
	return printUnits(res, asJSON)
}

func cmdAdd(a cliArgs) error {
	if len(a.positional) == 0 {
		return errors.New("使い方: mcpm add <名前> [--port <port>] [--idle <5m>] [--cwd <dir>] [--env KEY=VAL] -- <コマンド> [引数...]")
	}
	name := a.positional[0]
	cmdParts := a.rawAfterDD
	if len(cmdParts) == 0 && len(a.positional) > 1 {
		cmdParts = a.positional[1:]
	}
	if len(cmdParts) == 0 {
		return errors.New("実行するコマンドを指定してください（例: `mcpm add my-mcp -- pnpm dlx @modelcontextprotocol/server-everything`）")
	}

	cfg, err := core.LoadConfig()
	if err != nil {
		return err
	}

	sc := core.ServerConfig{
		Command:     cmdParts[0],
		Args:        cmdParts[1:],
		IdleTimeout: a.flags["idle"],
		Cwd:         a.flags["cwd"],
	}
	if pStr := a.flags["port"]; pStr != "" {
		port, err := strconv.Atoi(pStr)
		if err != nil || port <= 0 || port > 65535 {
			return fmt.Errorf("不正なポート番号です: %s", pStr)
		}
		sc.Port = port
	}
	if envs := a.multiFlags["env"]; len(envs) > 0 {
		sc.Env = map[string]string{}
		for _, kv := range envs {
			k, v, ok := strings.Cut(kv, "=")
			if !ok || k == "" {
				return fmt.Errorf("--env は KEY=VAL 形式で指定してください: %s", kv)
			}
			sc.Env[k] = v
		}
	}
	if a.flags["no-disc"] == "true" {
		f := false
		sc.CloseOnDisconnect = &f
	}

	cfg.Servers[name] = sc
	if err := core.SaveConfig(cfg); err != nil {
		return err
	}
	fmt.Printf("mcpm: %s を登録しました (%s)\n", name, core.ConfigPath())

	if isDaemonRunning() {
		var res core.PSResponse
		_ = call("POST", "/reload", core.ControlRequest{}, &res)
	}
	return nil
}

func cmdRemove(a cliArgs) error {
	if len(a.positional) == 0 {
		return errors.New("使い方: mcpm remove <MCPサーバー名>")
	}
	name := a.positional[0]
	cfg, err := core.LoadConfig()
	if err != nil {
		return err
	}
	if _, ok := cfg.Servers[name]; !ok {
		return fmt.Errorf("%q は登録されていません", name)
	}
	delete(cfg.Servers, name)
	if err := core.SaveConfig(cfg); err != nil {
		return err
	}
	_ = os.Remove(core.ServerCachePath(name))
	fmt.Printf("mcpm: %s を削除しました\n", name)

	if isDaemonRunning() {
		var res core.PSResponse
		_ = call("POST", "/reload", core.ControlRequest{}, &res)
	}
	return nil
}

func cmdImport(a cliArgs) error {
	if len(a.positional) == 0 {
		return errors.New("使い方: mcpm import <設定JSONファイル> [--base-port <開始ポート>]")
	}
	file := a.positional[0]
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}

	var wrapper struct {
		MCPServers map[string]core.ServerConfig `json:"mcpServers"`
	}
	var servers map[string]core.ServerConfig
	if err := json.Unmarshal(data, &wrapper); err == nil && len(wrapper.MCPServers) > 0 {
		servers = wrapper.MCPServers
	} else {
		if err := json.Unmarshal(data, &servers); err != nil {
			return fmt.Errorf("%s の解析に失敗しました: %w", file, err)
		}
	}

	cfg, err := core.LoadConfig()
	if err != nil {
		return err
	}

	basePort, _ := strconv.Atoi(a.flags["base-port"])
	count := 0
	for name, sc := range servers {
		if sc.Command == "" {
			continue
		}
		if basePort > 0 && sc.Port == 0 {
			sc.Port = basePort + count
		}
		cfg.Servers[name] = sc
		count++
	}
	if err := core.SaveConfig(cfg); err != nil {
		return err
	}
	fmt.Printf("mcpm: %d 個の MCP サーバーを %s にインポートしました\n", count, core.ConfigPath())

	if isDaemonRunning() {
		var res core.PSResponse
		_ = call("POST", "/reload", core.ControlRequest{}, &res)
	}
	return nil
}

func cmdClientConfig(a cliArgs) error {
	cfg, err := core.LoadConfig()
	if err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		exe = "mcpm"
	}
	useHTTP := a.flags["http"] == "true"
	gwPort := cfg.GatewayPort
	if gwPort == 0 {
		gwPort = core.DefaultGatewayPort
	}

	out := map[string]any{}
	servers := map[string]any{}
	for _, name := range cfg.SortedServerNames() {
		sc := cfg.Servers[name]
		if useHTTP {
			if sc.Port > 0 {
				servers[name] = map[string]any{
					"url": fmt.Sprintf("http://127.0.0.1:%d/mcp", sc.Port),
				}
			} else {
				servers[name] = map[string]any{
					"url": fmt.Sprintf("http://127.0.0.1:%d/mcp/%s", gwPort, name),
				}
			}
		} else {
			servers[name] = map[string]any{
				"command": exe,
				"args":    []string{"connect", name},
			}
		}
	}
	out["mcpServers"] = servers

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

func cmdLogs(a cliArgs) error {
	if len(a.positional) == 0 {
		return errors.New("使い方: mcpm logs <MCPサーバー名 | _daemon> [-n 行数] [-f]")
	}
	cfg, err := core.LoadConfig()
	if err != nil {
		return err
	}
	q := a.positional[0]
	name := cfg.Resolve(q)
	if name == "" {
		name = q
	}
	file := filepath.Join(core.LogDir(), name+".log")
	if _, err := os.Stat(file); err != nil {
		return fmt.Errorf("%s のログはまだありません（一度も起動されていません）", name)
	}
	n := a.flags["n"]
	if n == "" {
		n = "100"
	}
	tailArgs := []string{"-n", n}
	if a.flags["f"] == "true" {
		tailArgs = append(tailArgs, "-f")
	}
	tail, err := exec.LookPath("tail")
	if err != nil {
		return err
	}
	return syscall.Exec(tail, append([]string{"tail"}, append(tailArgs, file)...), os.Environ())
}

func isTerminal(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func isDaemonRunning() bool {
	c, err := net.DialTimeout("unix", core.SocketPath(), 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func call(method, path string, body any, out any) error {
	sock := core.SocketPath()
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequest(method, "http://pm"+path, &buf)
	if err != nil {
		return err
	}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("実プロセス (mcpmd) が起動していません。`mcpm start` を実行してください（%v）", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		var e struct{ Error string }
		_ = json.NewDecoder(res.Body).Decode(&e)
		return errors.New(e.Error)
	}
	if out != nil {
		return json.NewDecoder(res.Body).Decode(out)
	}
	return nil
}

func printUnits(res core.PSResponse, asJSON bool) error {
	if asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res.Units)
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tPORT\tSTATUS\tPID\tCLIENTS\tTOOLS\tCALLS\tSTARTED\tREASON")
	for _, u := range res.Units {
		port, pid, started, tools := "-", "-", "-", "-"
		if u.Port > 0 {
			port = strconv.Itoa(u.Port)
		}
		if u.Pid != 0 {
			pid = strconv.Itoa(u.Pid)
		}
		if u.Cached {
			tools = strconv.Itoa(u.CachedTools)
		}
		if t, err := time.Parse(time.RFC3339, u.StartedAt); err == nil {
			started = t.Local().Format("15:04:05")
		}
		st := u.Status
		if st == "idle" && u.Cached {
			st = "idle(cached)"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%d\t%s\t%s\n",
			u.Name, port, st, pid, u.ActiveClients, tools, u.TotalCalls, started, u.Reason)
	}
	return w.Flush()
}

func printUsage() {
	fmt.Print(`mcpm — MCP Compress / Process Manager (CLI / TUI クライアント)

mcpm 自体は操作・ブリッジ用のクライアント（叩き台）であり、
実プロセスである ` + "`mcpmd`" + `（バックグラウンド常駐デーモン）とは別プロセスで動作します。

コマンド:
  mcpm                                  mcpmd が未起動なら裏で起動し、TUI クライアントを開く（q で TUI だけ閉じる）
  mcpm start [--warm <name,...>] [--no-tui]
                                        実プロセス mcpmd を起動する
  mcpm tui                              稼働中の mcpmd に TUI で接続する
  mcpm ps [--format json]               状態一覧を表示する
  mcpm connect <name>                   MCP クライアント用 stdio ブリッジ（mcpmd 自動起動）
  mcpm up <name> [--reason <理由>]      手動で MCP プロセスを起動（ウォームアップ）
  mcpm close <name> [--reason <理由>]   MCP プロセスを kill して待機状態に戻す（stop も同義）
  mcpm restart <name> [--reason <理由>] MCP プロセスを再起動する
  mcpm refresh [<name>]                 tools/list 等のスキーマキャッシュを更新し kill する
  mcpm add <name> [--port <N>] [--idle <5m>] [--cwd <dir>] [--env K=V] -- <cmd> [args...]
                                        MCP サーバーを登録する
  mcpm remove <name>                    MCP サーバーの登録を削除する
  mcpm import <json-file> [--base-port <N>]
                                        既存の mcpServers 設定 JSON をインポートする
  mcpm client-config [--http]           クライアントに設定する mcpServers JSON を出力する
  mcpm logs <name> [-n <行数>] [-f]     ログを表示する（_daemon で mcpmd のログ）
  mcpm shutdown                         実プロセス (mcpmd) と全 MCP プロセスを完全停止する
`)
}
