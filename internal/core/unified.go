package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// MessageEndpoint は単一 MCP ユニットおよび統括（Unified）エンドポイントの共通インターフェース。
type MessageEndpoint interface {
	Subscribe() (uint64, <-chan []byte, func())
	HandleMessage(ctx context.Context, rawLine []byte) ([]byte, error)
}

type unifiedEndpoint struct {
	d *Daemon
}

func (d *Daemon) UnifiedEndpoint() MessageEndpoint {
	return &unifiedEndpoint{d: d}
}

func (d *Daemon) SortedUnits() []*MCPUnit {
	d.mu.Lock()
	defer d.mu.Unlock()
	names := d.cfg.SortedServerNames()
	units := make([]*MCPUnit, 0, len(names))
	for _, name := range names {
		if u, ok := d.units[name]; ok {
			units = append(units, u)
		}
	}
	return units
}

func (ue *unifiedEndpoint) Subscribe() (uint64, <-chan []byte, func()) {
	units := ue.d.SortedUnits()
	outCh := make(chan []byte, 64)
	done := make(chan struct{})
	var closeOnce sync.Once

	unsubs := make([]func(), 0, len(units))
	for _, u := range units {
		_, ch, unsub := u.Subscribe()
		unsubs = append(unsubs, unsub)
		go func(c <-chan []byte) {
			for {
				select {
				case <-done:
					return
				case msg, ok := <-c:
					if !ok {
						return
					}
					select {
					case outCh <- msg:
					default:
					}
				}
			}
		}(ch)
	}

	cleanup := func() {
		closeOnce.Do(func() {
			close(done)
			for _, fn := range unsubs {
				fn()
			}
			close(outCh)
		})
	}
	return 0, outCh, cleanup
}

type unitCachePair struct {
	unit  *MCPUnit
	cache *ServerCache
}

func (ue *unifiedEndpoint) collectCaches(ctx context.Context, forceRefresh bool) []unitCachePair {
	units := ue.d.SortedUnits()
	type resItem struct {
		idx   int
		unit  *MCPUnit
		cache *ServerCache
	}
	ch := make(chan resItem, len(units))
	for i, u := range units {
		go func(idx int, unit *MCPUnit) {
			c, err := unit.EnsureCache(ctx, forceRefresh)
			if err != nil {
				if ue.d.logger != nil {
					ue.d.logger.Printf("警告: %s のスキーマ取得をスキップしました: %v", unit.name, err)
				}
				ch <- resItem{idx: idx, unit: unit, cache: nil}
				return
			}
			ch <- resItem{idx: idx, unit: unit, cache: c}
		}(i, u)
	}

	ordered := make([]resItem, len(units))
	for range units {
		item := <-ch
		ordered[item.idx] = item
	}

	out := make([]unitCachePair, 0, len(units))
	for _, item := range ordered {
		if item.cache != nil {
			out = append(out, unitCachePair{unit: item.unit, cache: item.cache})
		}
	}
	return out
}

func (ue *unifiedEndpoint) HandleMessage(ctx context.Context, rawLine []byte) ([]byte, error) {
	var msg RPCMessage
	if err := json.Unmarshal(rawLine, &msg); err != nil {
		return MakeRPCError(json.RawMessage("null"), -32700, "Parse error")
	}

	if msg.IsNotification() {
		if msg.Method == "notifications/initialized" {
			return nil, nil
		}
		for _, u := range ue.d.SortedUnits() {
			_, _ = u.HandleMessage(ctx, rawLine)
		}
		return nil, nil
	}

	if !msg.IsRequest() {
		return nil, nil
	}

	switch msg.Method {
	case "ping", "logging/setLevel":
		return MakeRPCResult(msg.ID, map[string]any{})

	case "initialize":
		var clientParams struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(msg.Params, &clientParams)
		protoVer := clientParams.ProtocolVersion
		if protoVer == "" {
			protoVer = DefaultProtocolVer
		}

		pairs := ue.collectCaches(ctx, false)
		var instructions []string
		for _, p := range pairs {
			var initMeta struct {
				Instructions string `json:"instructions"`
			}
			if json.Unmarshal(p.cache.InitializeResult, &initMeta) == nil && strings.TrimSpace(initMeta.Instructions) != "" {
				instructions = append(instructions, fmt.Sprintf("[%s] %s", p.unit.name, strings.TrimSpace(initMeta.Instructions)))
			}
		}

		initObj := map[string]any{
			"protocolVersion": protoVer,
			"capabilities": map[string]any{
				"tools":     map[string]any{"listChanged": true},
				"resources": map[string]any{"listChanged": true},
				"prompts":   map[string]any{"listChanged": true},
			},
			"serverInfo": map[string]any{
				"name":    "mcpm",
				"version": "0.1.0",
			},
		}
		if len(instructions) > 0 {
			initObj["instructions"] = strings.Join(instructions, "\n")
		}
		return MakeRPCResult(msg.ID, initObj)

	case "tools/list":
		pairs := ue.collectCaches(ctx, false)
		nameCounts := map[string]int{}
		for _, p := range pairs {
			for _, rawTool := range p.cache.Tools {
				var t struct {
					Name string `json:"name"`
				}
				if json.Unmarshal(rawTool, &t) == nil && t.Name != "" {
					nameCounts[t.Name]++
				}
			}
		}

		tools := make([]json.RawMessage, 0)
		for _, p := range pairs {
			for _, rawTool := range p.cache.Tools {
				var tMap map[string]any
				if err := json.Unmarshal(rawTool, &tMap); err != nil {
					continue
				}
				origName, _ := tMap["name"].(string)
				if origName == "" {
					continue
				}
				if nameCounts[origName] > 1 {
					tMap["name"] = p.unit.name + "__" + origName
				}
				if desc, ok := tMap["description"].(string); ok && !strings.HasPrefix(desc, "["+p.unit.name+"]") {
					tMap["description"] = fmt.Sprintf("[%s] %s", p.unit.name, desc)
				}
				if b, err := json.Marshal(tMap); err == nil {
					tools = append(tools, b)
				}
			}
		}
		return MakeRPCResult(msg.ID, map[string]any{"tools": tools})

	case "resources/list":
		pairs := ue.collectCaches(ctx, false)
		res := make([]json.RawMessage, 0)
		for _, p := range pairs {
			res = append(res, p.cache.Resources...)
		}
		return MakeRPCResult(msg.ID, map[string]any{"resources": res})

	case "resources/templates/list":
		pairs := ue.collectCaches(ctx, false)
		tpls := make([]json.RawMessage, 0)
		for _, p := range pairs {
			tpls = append(tpls, p.cache.ResourceTemplates...)
		}
		return MakeRPCResult(msg.ID, map[string]any{"resourceTemplates": tpls})

	case "prompts/list":
		pairs := ue.collectCaches(ctx, false)
		prompts := make([]json.RawMessage, 0)
		for _, p := range pairs {
			prompts = append(prompts, p.cache.Prompts...)
		}
		return MakeRPCResult(msg.ID, map[string]any{"prompts": prompts})

	case "tools/call":
		var callParams map[string]any
		if err := json.Unmarshal(msg.Params, &callParams); err != nil {
			return MakeRPCError(msg.ID, -32602, "Invalid params")
		}
		toolName, _ := callParams["name"].(string)
		if toolName == "" {
			return MakeRPCError(msg.ID, -32602, "Missing tool name")
		}

		targetUnit, actualToolName := ue.resolveToolOwner(ctx, toolName)
		if targetUnit == nil {
			return MakeRPCError(msg.ID, -32601, fmt.Sprintf("ツール %q を提供する MCP サーバーが見つかりません", toolName))
		}

		forwardLine := rawLine
		if actualToolName != toolName {
			callParams["name"] = actualToolName
			if newParams, err := json.Marshal(callParams); err == nil {
				msg.Params = newParams
				if b, err := json.Marshal(msg); err == nil {
					forwardLine = b
				}
			}
		}
		return targetUnit.HandleMessage(ctx, forwardLine)

	case "resources/read", "resources/subscribe", "resources/unsubscribe":
		var resParams struct {
			URI string `json:"uri"`
		}
		_ = json.Unmarshal(msg.Params, &resParams)
		pairs := ue.collectCaches(ctx, false)
		for _, p := range pairs {
			for _, rawRes := range p.cache.Resources {
				var rItem struct {
					URI string `json:"uri"`
				}
				if json.Unmarshal(rawRes, &rItem) == nil && rItem.URI == resParams.URI {
					return p.unit.HandleMessage(ctx, rawLine)
				}
			}
		}
		if len(pairs) == 1 {
			return pairs[0].unit.HandleMessage(ctx, rawLine)
		}
		return MakeRPCError(msg.ID, -32601, fmt.Sprintf("リソース %q を提供する MCP サーバーが見つかりません", resParams.URI))

	case "prompts/get":
		var pParams map[string]any
		_ = json.Unmarshal(msg.Params, &pParams)
		pName, _ := pParams["name"].(string)
		if prefix, rest, ok := strings.Cut(pName, "__"); ok {
			if u := ue.d.ResolveUnit(prefix); u != nil {
				pParams["name"] = rest
				if newParams, err := json.Marshal(pParams); err == nil {
					msg.Params = newParams
					if b, err := json.Marshal(msg); err == nil {
						rawLine = b
					}
				}
				return u.HandleMessage(ctx, rawLine)
			}
		}
		pairs := ue.collectCaches(ctx, false)
		for _, p := range pairs {
			for _, rawPrompt := range p.cache.Prompts {
				var item struct {
					Name string `json:"name"`
				}
				if json.Unmarshal(rawPrompt, &item) == nil && item.Name == pName {
					return p.unit.HandleMessage(ctx, rawLine)
				}
			}
		}
		return MakeRPCError(msg.ID, -32601, fmt.Sprintf("プロンプト %q を提供する MCP サーバーが見つかりません", pName))

	default:
		return MakeRPCResult(msg.ID, map[string]any{})
	}
}

func (ue *unifiedEndpoint) resolveToolOwner(ctx context.Context, toolName string) (*MCPUnit, string) {
	if prefix, rest, ok := strings.Cut(toolName, "__"); ok && prefix != "" && rest != "" {
		if u := ue.d.ResolveUnit(prefix); u != nil {
			return u, rest
		}
	}

	// 1. まずメモリ上の既存キャッシュから高速検索
	for _, u := range ue.d.SortedUnits() {
		u.mu.Lock()
		c := u.cache
		u.mu.Unlock()
		if c != nil && cacheHasTool(c, toolName) {
			return u, toolName
		}
	}

	// 2. 未キャッシュのユニットがあればスキーマ取得を行って再検索
	pairs := ue.collectCaches(ctx, false)
	for _, p := range pairs {
		if cacheHasTool(p.cache, toolName) {
			return p.unit, toolName
		}
	}
	return nil, toolName
}

func cacheHasTool(c *ServerCache, want string) bool {
	if c == nil {
		return false
	}
	for _, rawTool := range c.Tools {
		var t struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(rawTool, &t) == nil && t.Name == want {
			return true
		}
	}
	return false
}
