# mcpm — MCP Compress / Process Manager

[English](#english) | [日本語](#日本語)

---

## English

**`mcpm`** is an on-demand process manager and multiplexer for [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) servers, written in Go.

When AI clients (Claude Code, Claude Desktop, Cursor, Antigravity, etc.) start up, they immediately send `initialize` and `tools/list` to every configured MCP server—spawning dozens of Node/Python processes across multiple editor windows even when you never call their tools.

**`mcpm` solves this:**
1. **Zero-Process Handshake & Schema Caching**: `mcpmd` immediately answers `initialize`, `ping`, `tools/list`, `resources/list`, and `prompts/list` from local schema cache (`~/.config/mcpm/cache/<name>.json`) with **0 MB memory overhead** and **0 running MCP processes**. (On the very first uncached run, it spawns the server once to cache the schema and immediately kills it.)
2. **Lazy Startup on Actual Call**: The real MCP server process is spawned **only** when an execution request (`tools/call`, `resources/read`, `prompts/get`) arrives. `mcpmd` transparently performs the MCP `initialize` handshake behind the scenes before forwarding the call.
3. **Coexistence / Multiplexing**: Multiple simultaneous clients (`mcpm connect <name>` over stdio or HTTP endpoints) share a **single** running instance of each MCP server via JSON-RPC ID multiplexing.
4. **Automatic & Manual Close (Kill)**:
	- **Idle Timeout**: Automatically kills the process group when idle for a configurable duration (default `5m`).
	- **Client Disconnect**: Automatically kills the process group once all connected clients disconnect and in-flight requests finish.
	- **Manual Close**: Run `mcpm close <name>` (or press `s` / `c` in the TUI) to kill the backend process while keeping client connections alive.
5. **Strict Separation of Client (`mcpm`) and Daemon (`mcpmd`)**:
	- `mcpm` is a pure CLI / TUI / stdio-bridge client—it never holds daemon state or child processes itself.
	- `mcpmd` is the standalone background daemon process that listens on a Unix Domain Socket and HTTP ports. Closing the `mcpm` TUI (`q`) only closes the TUI client while `mcpmd` and running MCP servers continue uninterrupted.

### Installation

#### Using `go install`

```bash
go install github.com/noir-chat-9661/mcpm/cmd/mcpm@latest
go install github.com/noir-chat-9661/mcpm/cmd/mcpmd@latest
```

#### Build from source

```bash
git clone https://github.com/noir-chat-9661/mcpm.git
cd mcpm
go build -o ~/.local/bin/mcpm ./cmd/mcpm
go build -o ~/.local/bin/mcpmd ./cmd/mcpmd
```

### Quick Start

```bash
# 1. Register MCP servers
mcpm add filesystem -- pnpm dlx @modelcontextprotocol/server-filesystem /path/to/workspace
mcpm add github --port 18801 --idle 3m --env GITHUB_PERSONAL_ACCESS_TOKEN=xxx -- pnpm dlx @modelcontextprotocol/server-github

# Or import from an existing .mcp.json / claude_desktop_config.json
mcpm import ~/.claude/.mcp.json

# 2. Generate client configuration JSON for Claude / Cursor / Antigravity
mcpm client-config          # stdio bridge mode (`mcpm connect <name>`)
mcpm client-config --http   # Streamable HTTP mode (`http://127.0.0.1:<port>/mcp`)

# 3. Open interactive TUI or check status
mcpm                        # Opens TUI (auto-starts mcpmd in background if not running)
mcpm ps                     # List status of all MCP servers
mcpm close filesystem       # Kill backend process and return to cached standby
```

### CLI Commands

| Command | Description |
| --- | --- |
| `mcpm` / `mcpm tui` | Open the interactive Bubble Tea TUI (auto-starts `mcpmd` if needed; `q` only closes TUI) |
| `mcpm start [--warm <names>] [--no-tui]` | Ensure the background `mcpmd` daemon is started |
| `mcpm ps [--format json]` | Show server status, PID, connected clients, cached tools count, and total calls |
| `mcpm connect <name>` | Stdio MCP bridge for MCP clients (auto-starts `mcpmd` if needed) |
| `mcpm up <name> [--reason <text>]` | Manually warm up / start an MCP server process |
| `mcpm close <name>` / `mcpm stop <name>` | Kill the backend MCP server process and return to cached standby |
| `mcpm restart <name> [--reason <text>]` | Restart the backend MCP server process |
| `mcpm refresh [<name>]` | Temporarily start server(s) to refresh cached `tools/list` schema, then kill |
| `mcpm add <name> [flags] -- <cmd> [args...]` | Register an MCP server in `~/.config/mcpm/config.json` |
| `mcpm remove <name>` | Remove an MCP server from config |
| `mcpm import <file> [--base-port <N>]` | Bulk import `mcpServers` from an existing JSON config file |
| `mcpm client-config [--http]` | Print `mcpServers` JSON snippet for your AI client |
| `mcpm logs <name> [-n <lines>] [-f]` | View logs (`_daemon` for `mcpmd` daemon log) |
| `mcpm shutdown` | Stop all MCP server processes and shut down `mcpmd` |

---

## 日本語

**`mcpm` (MCP Compress / Process Manager)** は、MCP (Model Context Protocol) サーバーを常駐させず、必要な瞬間だけ起動・共有し、使わなくなったら自動で `close` (kill) する Go 製プロセスマネージャーです。

### 特徴

1. **呼び出しまで起動しない（疎通・一覧は `mcpmd` がキャッシュから即答）**
	- クライアント起動時の `initialize`・`ping`・`tools/list`・`resources/list`・`prompts/list` は、`~/.config/mcpm/cache/<name>.json` のスキーマキャッシュから **メモリ消費 0MB・プロセス未起動のまま** 即答します。
	- 初回（未キャッシュ時）のみ 1 度だけプロセスを起動してスキーマを取得し、即座に自動 kill して待機状態（`◇ 疎通待機`）に戻ります。
2. **必要な時のみ起動 ＆ 複数クライアントで共存・共有**
	- `tools/call` 等の実呼び出しが来た瞬間にだけプロセスを起動し、裏で自動的に `initialize` ハンドシェイクを済ませてからリクエストを中継します。
	- 複数のクライアント（stdio の `mcpm connect <name>` や HTTP ポート経由）が同時に接続・呼び出しを行っても、単一のプロセスを JSON-RPC ID 多重化で安全に共有します。
3. **使わなくなったら `close` (kill)**
	- **アイドルタイムアウト**: 一定時間（デフォルト `5m`、`--idle` で変更可）呼び出しがなければプロセスグループごと自動 kill します。
	- **全クライアント切断時**: 接続中の全クライアントが切断され、実行中の呼び出しが 0 件になったら自動 kill します。
	- **手動 close**: `mcpm close <name>`（または TUI の `s` / `c` キー）で、クライアントとの接続を維持したまま裏のプロセスだけを kill できます。
4. **叩き台（`mcpm`）と実プロセス（`mcpmd`）の完全分離**
	- **`mcpm`（[`./cmd/mcpm`](./cmd/mcpm)）**: TUI 表示・CLI 操作・stdio ブリッジを行う純粋なクライアントです。自分自身は常駐プロセスを持ちません。TUI を `q` で閉じても裏の `mcpmd` や MCP プロセスは停止しません。
	- **`mcpmd`（[`./cmd/mcpmd`](./cmd/mcpmd)）**: UDS および HTTP ポートを Listen し、MCP プロセス群を管理するバックグラウンドデーモン（実プロセス）です。未起動時に `mcpm` や `mcpm connect` が呼ばれると自動的に別プロセスとして立ち上がります。

### インストール

#### `go install` を使う場合

```bash
go install github.com/noir-chat-9661/mcpm/cmd/mcpm@latest
go install github.com/noir-chat-9661/mcpm/cmd/mcpmd@latest
```

#### ソースからビルドする場合

```bash
git clone https://github.com/noir-chat-9661/mcpm.git
cd mcpm
go build -o ~/.local/bin/mcpm ./cmd/mcpm
go build -o ~/.local/bin/mcpmd ./cmd/mcpmd
```

### クイックスタート

```bash
# 1. MCP サーバーの登録
mcpm add filesystem -- pnpm dlx @modelcontextprotocol/server-filesystem /path/to/workspace
mcpm add github --port 18801 --idle 3m --env GITHUB_PERSONAL_ACCESS_TOKEN=xxx -- pnpm dlx @modelcontextprotocol/server-github

# 既存の .mcp.json や claude_desktop_config.json から一括インポート
mcpm import ~/.claude/.mcp.json

# 2. クライアント設定用 JSON の出力
mcpm client-config          # stdio ブリッジ形式 (`mcpm connect <name>`)
mcpm client-config --http   # Streamable HTTP URL 形式 (`http://127.0.0.1:<port>/mcp`)

# 3. TUI 操作・状態確認・close
mcpm                        # TUI を開く（mcpmd 未起動なら裏で自動起動）
mcpm ps                     # 状態一覧
mcpm close filesystem       # プロセスを kill して疎通待機状態に戻す
mcpm shutdown               # 実プロセス (mcpmd) と全 MCP サーバーを完全停止
```

### TUI キー操作

- `↑` / `↓` (`k` / `j`): 対象の MCP サーバーを選択
- `u`: 起動（ウォームアップ）
- `s` / `c`: `close`（プロセスを kill して `◇ 疎通待機` に戻す）
- `r`: 再起動
- `i`: スキーマキャッシュ再取得（取得後に未使用なら自動 kill）
- `q` / `Ctrl-C`: **TUI クライアントだけを閉じる**（裏の `mcpmd` と MCP プロセスは継続）
- `D`: 実プロセス `mcpmd` と全 MCP サーバーを停止して終了

## License

[MIT License](./LICENSE)
