package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/noir-chat-9661/mcpm/internal/core"
)

// remoteSource は別プロセスで動く mcpmd に対して UDS 越しに状態取得・操作を行う。
// mcpm (TUI) 自体は実プロセスを持たず、純粋なクライアントとして振る舞う。
type remoteSource struct{}

func (s remoteSource) units() (core.PSResponse, error) {
	var res core.PSResponse
	err := call("GET", "/ps", nil, &res)
	return res, err
}

func (s remoteSource) events(since int) ([]core.Event, error) {
	var res struct{ Events []core.Event }
	err := call("GET", "/events?since="+strconv.Itoa(since), nil, &res)
	return res.Events, err
}

func (s remoteSource) act(action, name string) error {
	var res core.PSResponse
	return call("POST", "/"+action, core.ControlRequest{Unit: name, Reason: "TUI 操作"}, &res)
}

func (s remoteSource) shutdownDaemon() error {
	var res struct{ OK bool }
	return call("POST", "/shutdown", nil, &res)
}

func runTUI() error {
	if !isTerminal(os.Stdin) || !isTerminal(os.Stdout) {
		return errors.New("端末（TTY）ではないため TUI を起動できません（`mcpm ps` を使用してください）")
	}
	p := tea.NewProgram(model{src: remoteSource{}, width: 100, height: 24})
	_, err := p.Run()
	return err
}

type tickMsg time.Time

type refreshMsg struct {
	ps     core.PSResponse
	events []core.Event
	mem    memUsage
	err    error
}

type actDoneMsg string

const refreshInterval = 500 * time.Millisecond

func tick() tea.Cmd {
	return tea.Tick(refreshInterval, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func refresh(src remoteSource, since int) tea.Cmd {
	return func() tea.Msg {
		ps, err := src.units()
		var evs []core.Event
		if err == nil {
			evs, err = src.events(since)
		}
		return refreshMsg{ps: ps, events: evs, mem: readMemory(), err: err}
	}
}

type model struct {
	src         remoteSource
	list        []core.UnitStatus
	daemonPid   int
	gatewayPort int
	events      []core.Event
	lastSeq     int
	mem         memUsage
	selected    int
	message     string
	err         error
	width       int
	height      int
	frame       int
}

func (m model) Init() tea.Cmd {
	return tea.Batch(refresh(m.src, 0), tick())
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
	case tickMsg:
		m.frame++
		return m, tea.Batch(refresh(m.src, m.lastSeq), tick())
	case refreshMsg:
		m.err = msg.err
		if msg.err != nil {
			return m, nil
		}
		m.list = msg.ps.Units
		m.daemonPid = msg.ps.DaemonPid
		m.gatewayPort = msg.ps.GatewayPort
		m.mem = msg.mem
		m.selected = min(m.selected, max(0, len(m.list)-1))
		for _, e := range msg.events {
			if e.Seq > m.lastSeq {
				m.events = append(m.events, e)
				m.lastSeq = e.Seq
			}
		}
		if over := len(m.events) - 200; over > 0 {
			m.events = m.events[over:]
		}
	case actDoneMsg:
		m.message = string(msg)
	case tea.KeyPressMsg:
		switch msg.String() {
		case "q", "ctrl+c":
			// TUI クライアントだけを閉じる（実プロセス mcpmd と稼働中の MCP サーバーはそのまま継続する）
			return m, tea.Quit
		case "D":
			// 明示的に D を押したときのみ実プロセス mcpmd ごと停止して終了する
			_ = m.src.shutdownDaemon()
			return m, tea.Quit
		case "up", "k":
			m.selected = max(0, m.selected-1)
		case "down", "j":
			m.selected = min(max(0, len(m.list)-1), m.selected+1)
		case "u", "r", "s", "c", "i":
			if m.selected >= len(m.list) {
				return m, nil
			}
			return m, m.act(msg.String(), m.list[m.selected].Name)
		}
	}
	return m, nil
}

func (m *model) act(key, name string) tea.Cmd {
	action := map[string]string{
		"u": "start",
		"r": "restart",
		"s": "stop",
		"c": "close",
		"i": "refresh",
	}[key]
	label := map[string]string{
		"start":   "起動",
		"restart": "再起動",
		"stop":    "close (kill)",
		"close":   "close (kill)",
		"refresh": "スキーマ更新",
	}[action]
	m.message = fmt.Sprintf("%s を%sしています…", name, label)
	src := m.src
	return func() tea.Msg {
		if err := src.act(action, name); err != nil {
			return actDoneMsg(fmt.Sprintf("%s の%sに失敗しました: %v", name, label, err))
		}
		return actDoneMsg(fmt.Sprintf("%s を%sしました", name, label))
	}
}

var (
	styleBold   = lipgloss.NewStyle().Bold(true)
	styleDim    = lipgloss.NewStyle().Faint(true)
	styleGreen  = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleYellow = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	styleRed    = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	styleCyan   = lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
	styleCursor = lipgloss.NewStyle().Foreground(lipgloss.Color("6")).Bold(true)
)

var spinner = []string{"◐", "◓", "◑", "◒"}

func (m model) statusCell(u core.UnitStatus) (lipgloss.Style, string) {
	frame := spinner[m.frame%len(spinner)]
	switch u.Status {
	case "running":
		return styleGreen, "● 稼働中"
	case "starting":
		return styleYellow, frame + " 起動中"
	case "stopping":
		return styleYellow, frame + " 停止中"
	case "failed":
		return styleRed, "✕ 失敗"
	case "stopped":
		return styleDim, "○ 停止"
	default:
		if u.Cached {
			return styleCyan, "◇ 疎通待機"
		}
		return styleDim, "○ 未取得"
	}
}

func fit(s string, w int) string {
	if w <= 0 {
		return ""
	}
	s = ansi.Truncate(s, w, "")
	if pad := w - lipgloss.Width(s); pad > 0 {
		s += strings.Repeat(" ", pad)
	}
	return s
}

func rule(title string, w int) string {
	head := "── " + title + " "
	return styleDim.Render(head + strings.Repeat("─", max(0, w-lipgloss.Width(head))))
}

func (m model) View() tea.View {
	v := tea.NewView(m.render())
	v.AltScreen = true
	v.WindowTitle = "mcpm"
	return v
}

func (m model) render() string {
	w, h := m.width, m.height
	var lines []string
	add := func(s string) { lines = append(lines, s) }

	running := 0
	totalMem := 0
	for _, u := range m.list {
		if u.Status == "running" {
			running++
		}
		if u.Pid != 0 {
			totalMem += m.mem.byPgid[u.Pid]
		}
	}

	daemonInfo := ""
	if m.daemonPid > 0 {
		daemonInfo = fmt.Sprintf("   mcpmd(PID %d)", m.daemonPid)
	}
	gwInfo := ""
	if m.gatewayPort > 0 {
		gwInfo = fmt.Sprintf("   Gateway 127.0.0.1:%d", m.gatewayPort)
	}
	add(styleBold.Render("mcpm") +
		fmt.Sprintf("   稼働中 %d / %d   MCPメモリ %s%s%s", running, len(m.list), orDash(fmtMem(totalMem)), daemonInfo, gwInfo))
	if m.err != nil {
		add(styleRed.Render("mcpmd に接続できません: " + m.err.Error()))
	} else {
		add(styleDim.Render("TUI は操作用のクライアントです（q で閉じても裏の mcpmd と MCP プロセスは継続します）。"))
	}

	const nameW, portW, statW, cliW, toolW, callW, upW, memW = 22, 7, 12, 7, 7, 6, 8, 8
	reasonW := max(12, w-2-nameW-portW-statW-cliW-toolW-callW-upW-memW)
	add(styleDim.Render("  " +
		fit("名前", nameW) +
		fit("ポート", portW) +
		fit("状態", statW) +
		fit("接続", cliW) +
		fit("ツール", toolW) +
		fit("呼出", callW) +
		fit("稼働", upW) +
		fit("メモリ", memW) +
		fit("理由 / 状況", reasonW)))

	if len(m.list) == 0 {
		add(styleDim.Render("  登録されている MCP サーバーがありません。`mcpm add` または `mcpm import` で追加してください。"))
	}

	for i, u := range m.list {
		isSelected := i == m.selected
		marker := "  "
		if isSelected {
			marker = styleCursor.Render("› ")
		}
		stStyle, st := m.statusCell(u)

		up := ""
		if (u.Status == "running" || u.Status == "starting") && u.StartedAt != "" {
			if ts, err := time.Parse(time.RFC3339, u.StartedAt); err == nil {
				up = fmtDur(time.Since(ts))
			}
		}

		mem := ""
		if u.Pid != 0 {
			mem = fmtMem(m.mem.byPgid[u.Pid])
		}

		portStr := "-"
		if u.Port > 0 {
			portStr = strconv.Itoa(u.Port)
		}

		cliStr := strconv.Itoa(u.ActiveClients)
		if u.InFlight > 0 {
			cliStr = fmt.Sprintf("%d(%d)", u.ActiveClients, u.InFlight)
		}

		toolStr := "-"
		if u.Cached {
			toolStr = strconv.Itoa(u.CachedTools)
		}

		callStr := strconv.Itoa(u.TotalCalls)

		detail := u.Reason
		if u.Status == "failed" {
			if errLine := latestLine(u.Name, true); errLine != "" {
				detail = errLine
			}
		}

		textStyle := lipgloss.NewStyle()
		if isSelected {
			textStyle = textStyle.Bold(true)
		}
		detailStyle := textStyle
		if u.Status == "failed" {
			detailStyle = styleRed
		}

		add(marker +
			textStyle.Render(fit(u.Name, nameW)+fit(portStr, portW)) +
			stStyle.Render(fit(st, statW)) +
			textStyle.Render(
				fit(cliStr, cliW)+
					fit(toolStr, toolW)+
					fit(callStr, callW)+
					fit(up, upW)+
					fit(mem, memW)) +
			detailStyle.Render(fit(detail, reasonW)))
	}

	add("")
	add(rule("イベント", w))
	evLines := importantEvents(m.events, eventLines)
	for i := range eventLines {
		if i < len(evLines) {
			add(colorEvent(fit(evLines[i], w)))
		} else {
			add("")
		}
	}

	add(styleDim.Render("↑↓ 移動   u 起動   s close(kill)   r 再起動   i スキーマ更新   q TUIを閉じる   D mcpmd全停止"))
	if m.message != "" {
		add(fit(m.message, w))
	} else {
		add("")
	}

	if len(lines) > h {
		lines = lines[:h]
	}
	return strings.Join(lines, "\n")
}

const eventLines = 5

func importantEvents(evs []core.Event, n int) []string {
	var out []string
	for i := len(evs) - 1; i >= 0 && len(out) < n; i-- {
		l := evs[i].Line
		for _, k := range []string{"起動", "準備完了", "失敗", "停止", "警告", "設定"} {
			if strings.Contains(l, k) {
				out = append([]string{l}, out...)
				break
			}
		}
	}
	return out
}

func colorEvent(line string) string {
	switch {
	case strings.Contains(line, "失敗"):
		return styleRed.Render(line)
	case strings.Contains(line, "準備完了"):
		return styleGreen.Render(line)
	case strings.Contains(line, "起動") || strings.Contains(line, "警告"):
		return styleYellow.Render(line)
	case strings.Contains(line, "停止"):
		return styleDim.Render(line)
	}
	return line
}

var ansiRe = regexp.MustCompile(`\x1b\[[0-9;?]*[A-Za-z]`)

type memUsage struct {
	byPid  map[int]int
	byPgid map[int]int
}

func readMemory() memUsage {
	m := memUsage{byPid: map[int]int{}, byPgid: map[int]int{}}
	out, err := exec.Command("ps", "-axo", "pid=,pgid=,rss=").Output()
	if err != nil {
		return m
	}
	for _, line := range strings.Split(string(out), "\n") {
		f := strings.Fields(line)
		if len(f) != 3 {
			continue
		}
		pid, _ := strconv.Atoi(f[0])
		pgid, _ := strconv.Atoi(f[1])
		rss, _ := strconv.Atoi(f[2])
		m.byPid[pid] = rss
		m.byPgid[pgid] += rss
	}
	return m
}

func tailLog(name string, n int) []string {
	f, err := os.Open(filepath.Join(core.LogDir(), name+".log"))
	if err != nil {
		return nil
	}
	defer f.Close()
	const chunk = 32 * 1024
	st, err := f.Stat()
	if err != nil {
		return nil
	}
	off := max(0, st.Size()-chunk)
	buf := make([]byte, st.Size()-off)
	if _, err := f.ReadAt(buf, off); err != nil {
		return nil
	}
	var lines []string
	for _, l := range strings.Split(string(buf), "\n") {
		l = strings.TrimRight(ansiRe.ReplaceAllString(l, ""), "\r ")
		if strings.TrimSpace(l) == "" || strings.HasPrefix(l, "=== mcpm:") {
			continue
		}
		lines = append(lines, l)
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return lines
}

func latestLine(name string, failed bool) string {
	lines := tailLog(name, 40)
	if failed {
		for i := len(lines) - 1; i >= 0; i-- {
			if strings.Contains(strings.ToLower(lines[i]), "error") {
				return strings.TrimSpace(lines[i])
			}
		}
	}
	if len(lines) == 0 {
		return ""
	}
	return strings.TrimSpace(lines[len(lines)-1])
}

func fmtMem(kb int) string {
	switch {
	case kb <= 0:
		return ""
	case kb >= 1024*1024:
		return fmt.Sprintf("%.1fGB", float64(kb)/1024/1024)
	default:
		return fmt.Sprintf("%dMB", max(1, kb/1024))
	}
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
