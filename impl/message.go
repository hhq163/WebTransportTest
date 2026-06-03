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

// OneWayDelayMs 计算从发送到当前的单向延迟（毫秒）
func (m *Message) OneWayDelayMs() int64 {
	if m.SendTimeMs <= 0 {
		return -1
	}
	return time.Now().UnixMilli() - m.SendTimeMs
}
