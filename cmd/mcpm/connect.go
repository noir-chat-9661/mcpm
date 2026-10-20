package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/noir-chat-9661/mcpm/internal/core"
)

func resolveDaemonBinary() (string, error) {
	if exe, err := os.Executable(); err == nil {
		sibling := filepath.Join(filepath.Dir(exe), "mcpmd")
		if st, err := os.Stat(sibling); err == nil && !st.IsDir() {
			return sibling, nil
		}
	}
	if p, err := exec.LookPath("mcpmd"); err == nil {
		return p, nil
	}
	return "", errors.New("mcpmd バイナリが見つかりません（mcpm と同じディレクトリまたは PATH に mcpmd を配置してください）")
}

// ensureDaemonRunning は実プロセス (mcpmd) が未起動の場合に別プロセスとしてバックグラウンド起動し、UDS 接続を返す。
func ensureDaemonRunning(warm []string) (net.Conn, error) {
	sock := core.SocketPath()
	if c, err := net.Dial("unix", sock); err == nil {
		return c, nil
	}

	if err := spawnDetachedDaemon(warm); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.Dial("unix", sock); err == nil {
			return c, nil
		}
		time.Sleep(60 * time.Millisecond)
	}
	return nil, errors.New("mcpmd（実プロセス）の起動待ちがタイムアウトしました")
}

func spawnDetachedDaemon(warm []string) error {
	if err := core.EnsureHomeDir(); err != nil {
		return err
	}
	daemonBin, err := resolveDaemonBinary()
	if err != nil {
		return err
	}
	lf, err := os.OpenFile(filepath.Join(core.LogDir(), "_daemon.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer lf.Close()

	var args []string
	if len(warm) > 0 {
		args = append(args, "--warm", strings.Join(warm, ","))
	}
	cmd := exec.Command(daemonBin, args...)
	cmd.Stdout = lf
	cmd.Stderr = lf
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd.Start()
}

// runConnect は MCP クライアント（stdio）と別プロセスの mcpmd（UDS）をつなぐブリッジを実行する。
func runConnect(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("使い方: mcpm connect <MCPサーバー名>")
	}

	conn, err := ensureDaemonRunning(nil)
	if err != nil {
		return err
	}
	defer conn.Close()

	reqLine := fmt.Sprintf("POST /connect?name=%s HTTP/1.1\r\nHost: pm\r\n\r\n", url.QueryEscape(name))
	if _, err := io.WriteString(conn, reqLine); err != nil {
		return err
	}

	br := bufio.NewReaderSize(conn, 64*1024)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		return fmt.Errorf("mcpmd からの応答に失敗しました: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		var e struct{ Error string }
		_ = json.NewDecoder(resp.Body).Decode(&e)
		if e.Error != "" {
			return errors.New(e.Error)
		}
		return fmt.Errorf("mcpmd error: status %d", resp.StatusCode)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(os.Stdout, br)
	}()

	go func() {
		_, _ = io.Copy(conn, os.Stdin)
		if uc, ok := conn.(*net.UnixConn); ok {
			_ = uc.CloseWrite()
		} else {
			_ = conn.Close()
		}
	}()

	<-done
	return nil
}
