// mcpmd: mcpm の実プロセス（バックグラウンド常駐デーモン）。
// TUI や CLI クライアント（mcpm）とは完全に別プロセスとして動作し、
// UDS および HTTP ポートを Listen して MCP サーバーの遅延起動・共有・kill を管理する。
package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/noir-chat-9661/mcpm/internal/core"
)

func main() {
	var warm []string
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		s := args[i]
		if s == "--warm" && i+1 < len(args) {
			for _, w := range strings.Split(args[i+1], ",") {
				if w = strings.TrimSpace(w); w != "" {
					warm = append(warm, w)
				}
			}
			i++
		} else if after, ok := strings.CutPrefix(s, "--warm="); ok {
			for _, w := range strings.Split(after, ",") {
				if w = strings.TrimSpace(w); w != "" {
					warm = append(warm, w)
				}
			}
		}
	}

	cfg, err := core.LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "mcpmd:", err)
		os.Exit(1)
	}
	if err := core.RunDaemon(cfg, warm); err != nil {
		fmt.Fprintln(os.Stderr, "mcpmd:", err)
		os.Exit(1)
	}
}
