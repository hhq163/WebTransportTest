package base

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
)

// Message 是所有子项目（client、server、wsclient、wsserver）共用的消息结构
type Message struct {
	Type       string `json:"type"`
	Seq        int64  `json:"seq"`          // 全局唯一序号，用于到达率统计
	Payload    []byte `json:"payload"`      // 负载数据
	SendTimeMs int64  `json:"send_time_ms"` // 发送端 Unix 毫秒时间戳，客户端填写
}

// NewMessage 创建一条携带当前时间戳的消息
func NewMessage(msgType string, seq int64, payload []byte) *Message {
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

// WriteMessage 写入带长度前缀的消息到流
// 格式：[4字节长度][JSON消息]
func WriteMessage(stream *webtransport.Stream, msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// 写入长度前缀（4字节，big-endian）
	lengthPrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthPrefix, uint32(len(data)))

	// 先写长度
	if _, err := stream.Write(lengthPrefix); err != nil {
		return err
	}
	// 再写消息内容
	if _, err := stream.Write(data); err != nil {
		return err
	}
	return nil
}

// ReadMessage 从流中读取带长度前缀的消息
// 格式：[4字节长度][JSON消息]
func ReadMessage(stream *webtransport.Stream) (*Message, error) {
	// 读取长度前缀
	lengthPrefix := make([]byte, 4)
	if _, err := io.ReadFull(stream, lengthPrefix); err != nil {
		return nil, err
	}

	// 解析消息长度
	length := binary.BigEndian.Uint32(lengthPrefix)
	if length > 10*1024*1024 { // 限制最大10MB
		return nil, fmt.Errorf("message too large: %d bytes", length)
	}

	// 读取完整消息
	data := make([]byte, length)
	if _, err := io.ReadFull(stream, data); err != nil {
		return nil, err
	}

	// 解析JSON
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}

	return &msg, nil
}
