package base

import (
	"encoding/json"
	"time"
)

// Message 是所有子项目（client、server、wsclient、wsserver）共用的消息结构
type Message struct {
	Type       string          `json:"type"`
	Seq        int64           `json:"seq"`          // 全局唯一序号，用于到达率统计
	Payload    json.RawMessage `json:"payload"`
	SendTimeMs int64           `json:"send_time_ms"` // 发送端 Unix 毫秒时间戳，客户端填写
}

// NewMessage 创建一条携带当前时间戳的消息
func NewMessage(msgType string, seq int64, payload json.RawMessage) *Message {
	return &Message{
		Type:       msgType,
		Seq:        seq,
		Payload:    payload,
		SendTimeMs: time.Now().UnixMilli(),
	}
}

// RTTMs 收到回包时计算往返延迟（毫秒），所有时间在客户端本地计算
func RTTMs(sendTimeMs int64) int64 {
	if sendTimeMs <= 0 {
		return -1
	}
	return time.Now().UnixMilli() - sendTimeMs
}

// OneWayDelayMs 计算单向延迟（毫秒），要求双端时钟同步
func (m *Message) OneWayDelayMs() int64 {
	if m.SendTimeMs <= 0 {
		return -1
	}
	return time.Now().UnixMilli() - m.SendTimeMs
}
