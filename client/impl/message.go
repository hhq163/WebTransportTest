package impl

import (
	"encoding/json"
	"time"
)

// Message 是客户端/服务端通信的统一消息结构
type Message struct {
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
	SendTimeMs int64           `json:"send_time_ms"` // 发送端 Unix 毫秒时间戳
}

// NewMessage 创建一条携带当前时间戳的消息
func NewMessage(msgType string, payload json.RawMessage) *Message {
	return &Message{
		Type:       msgType,
		Payload:    payload,
		SendTimeMs: time.Now().UnixMilli(),
	}
}

// RTTMs 在收到响应时用于计算往返延迟（毫秒）
// sendTimeMs 为发送时记录的时间戳
func RTTMs(sendTimeMs int64) int64 {
	if sendTimeMs <= 0 {
		return -1
	}
	return time.Now().UnixMilli() - sendTimeMs
}
