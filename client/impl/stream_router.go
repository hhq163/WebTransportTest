package impl

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"

	webtransport "github.com/quic-go/webtransport-go"
)

// StreamHandler 定义某种消息类型收到响应后的处理回调
type StreamHandler func(msgType string, resp []byte, stats *Stats)

// streamEntry 持有一条长生命周期的双向流及其接收循环
type streamEntry struct {
	stream  *webtransport.Stream
	msgType string
	send    chan []byte // 发送队列，串行写防止并发写流
	done    chan struct{}
}

// StreamRouter 管理多条按消息类型复用的长生命周期双向流
// 每种 msgType 对应一条固定的 stream，所有同类消息在该流上顺序收发
type StreamRouter struct {
	session  *webtransport.Session
	stats    *Stats
	handler  StreamHandler
	mu       sync.Mutex
	routes   map[string]*streamEntry
}

func NewStreamRouter(session *webtransport.Session, stats *Stats, handler StreamHandler) *StreamRouter {
	return &StreamRouter{
		session: session,
		stats:   stats,
		handler: handler,
		routes:  make(map[string]*streamEntry),
	}
}

// Open 为 msgType 建立一条专用双向流，重复调用幂等
func (r *StreamRouter) Open(msgType string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.routes[msgType]; ok {
		return nil
	}

	stream, err := r.session.OpenStream()
	if err != nil {
		return fmt.Errorf("open stream [%s]: %w", msgType, err)
	}

	entry := &streamEntry{
		stream:  stream,
		msgType: msgType,
		send:    make(chan []byte, 64),
		done:    make(chan struct{}),
	}
	r.routes[msgType] = entry

	// 写协程：串行消费 send 队列，避免并发写同一条流
	go func() {
		defer stream.Close()
		defer close(entry.done)
		for data := range entry.send {
			if err := WriteWithRetry(stream, data, r.stats, 3); err != nil {
				log.Printf("[router:%s] 写入失败: %v", msgType, err)
				return
			}
		}
	}()

	// 读协程：持续接收该流上的响应
	go func() {
		buf := make([]byte, 65536)
		for {
			n, err := stream.Read(buf)
			if err != nil {
				if err != io.EOF {
					log.Printf("[router:%s] 读取失败: %v", msgType, err)
				}
				return
			}
			if n > 0 {
				r.stats.RecordReceived()
				resp := make([]byte, n)
				copy(resp, buf[:n])
				if r.handler != nil {
					r.handler(msgType, resp, r.stats)
				}
			}
		}
	}()

	log.Printf("[router] 建立流 type=%s", msgType)
	return nil
}

// Send 将消息路由到对应 msgType 的流，若流不存在则自动建立
func (r *StreamRouter) Send(msg *Message) error {
	if err := r.Open(msg.Type); err != nil {
		return err
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	r.mu.Lock()
	entry := r.routes[msg.Type]
	r.mu.Unlock()

	select {
	case entry.send <- data:
		return nil
	case <-entry.done:
		return fmt.Errorf("stream [%s] already closed", msg.Type)
	}
}

// Close 关闭所有流
func (r *StreamRouter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entry := range r.routes {
		close(entry.send)
	}
	r.routes = make(map[string]*streamEntry)
}
