package core

import (
	"strings"
	"sync"
)

const eventCapacity = 300

type Event struct {
	Seq  int    `json:"seq"`
	Line string `json:"line"`
}

type EventLog struct {
	mu      sync.Mutex
	seq     int
	lines   []Event
	partial strings.Builder
}

func (e *EventLog) Write(p []byte) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.partial.Write(p)
	s := e.partial.String()
	for {
		i := strings.IndexByte(s, '\n')
		if i < 0 {
			break
		}
		e.seq++
		e.lines = append(e.lines, Event{Seq: e.seq, Line: s[:i]})
		s = s[i+1:]
	}
	if over := len(e.lines) - eventCapacity; over > 0 {
		e.lines = append([]Event(nil), e.lines[over:]...)
	}
	e.partial.Reset()
	e.partial.WriteString(s)
	return len(p), nil
}

func (e *EventLog) Since(seq int) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := []Event{}
	for _, ev := range e.lines {
		if ev.Seq > seq {
			out = append(out, ev)
		}
	}
	return out
}
