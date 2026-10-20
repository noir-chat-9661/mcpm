package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

type RPCMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (m *RPCMessage) IsRequest() bool {
	return m.Method != "" && len(m.ID) > 0 && string(m.ID) != "null"
}

func (m *RPCMessage) IsNotification() bool {
	return m.Method != "" && (len(m.ID) == 0 || string(m.ID) == "null")
}

func (m *RPCMessage) IsResponse() bool {
	return m.Method == "" && len(m.ID) > 0 && string(m.ID) != "null"
}

func MakeRPCResult(id json.RawMessage, result any) ([]byte, error) {
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	resp := RPCMessage{
		JSONRPC: "2.0",
		ID:      id,
		Result:  raw,
	}
	return json.Marshal(resp)
}

func MakeRPCError(id json.RawMessage, code int, message string) ([]byte, error) {
	resp := RPCMessage{
		JSONRPC: "2.0",
		ID:      id,
		Error: &RPCError{
			Code:    code,
			Message: message,
		},
	}
	return json.Marshal(resp)
}

type ServerCache struct {
	ServerName        string            `json:"serverName"`
	UpdatedAt         time.Time         `json:"updatedAt"`
	InitializeResult  json.RawMessage   `json:"initializeResult"`
	Tools             []json.RawMessage `json:"tools"`
	Resources         []json.RawMessage `json:"resources,omitempty"`
	ResourceTemplates []json.RawMessage `json:"resourceTemplates,omitempty"`
	Prompts           []json.RawMessage `json:"prompts,omitempty"`
}

func ServerCachePath(name string) string {
	return filepath.Join(CacheDir(), name+".json")
}

func LoadServerCache(name string) (*ServerCache, error) {
	data, err := os.ReadFile(ServerCachePath(name))
	if err != nil {
		return nil, err
	}
	var c ServerCache
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if c.Tools == nil {
		c.Tools = []json.RawMessage{}
	}
	return &c, nil
}

func SaveServerCache(c *ServerCache) error {
	if err := EnsureHomeDir(); err != nil {
		return err
	}
	if c.Tools == nil {
		c.Tools = []json.RawMessage{}
	}
	data, err := json.MarshalIndent(c, "", "\t")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(ServerCachePath(c.ServerName), data, 0o600)
}

func IsLightweightMethod(method string) bool {
	switch method {
	case "initialize",
		"ping",
		"logging/setLevel",
		"tools/list",
		"resources/list",
		"resources/templates/list",
		"prompts/list":
		return true
	default:
		return false
	}
}
