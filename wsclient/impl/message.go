package impl

import (
	"encoding/json"
	"time"
)

// Message 与 WebTransport 客户端保持相同结构，便于对比测试
type Message struct {
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	SendTimeMs int64           `json:"send_time_ms"`
}

func NewMessage(msgType string, payload json.RawMessage) *Message {
	return &Message{
		Type:       msgType,
		Payload:    payload,
		SendTimeMs: time.Now().UnixMilli(),
	}
}

// RTTMs 收到回包时计算往返延迟，所有时间在客户端计算
func RTTMs(sendTimeMs int64) int64 {
	if sendTimeMs <= 0 {
		return -1
	}
	return time.Now().UnixMilli() - sendTimeMs
}
