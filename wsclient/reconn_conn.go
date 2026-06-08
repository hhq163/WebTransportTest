package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// reconnConn 管理单条 WebSocket 连接，自动断线重连
// sendLoop 只与 ch 交互，不感知底层连接状态
type reconnConn struct {
	label  string
	dialFn func() (*websocket.Conn, error)
	onRecv func([]byte)     // 收到消息的回调
	ch     chan []byte       // 发送队列
	ctx    context.Context

	mu   sync.Mutex
	conn *websocket.Conn
}

func newReconnConn(ctx context.Context, label string, dialFn func() (*websocket.Conn, error), onRecv func([]byte)) *reconnConn {
	rc := &reconnConn{
		label:  label,
		dialFn: dialFn,
		onRecv: onRecv,
		ch:     make(chan []byte, 512),
		ctx:    ctx,
	}
	go rc.run()
	return rc
}

// run 管理连接生命周期：建连 → 启动读写 → 断线退避重连
func (rc *reconnConn) run() {
	backoff := 500 * time.Millisecond
	const maxBackoff = 10 * time.Second

	for {
		if rc.ctx.Err() != nil {
			return
		}

		conn, err := rc.dialFn()
		if err != nil {
			log.Printf("[%s] 连接失败: %v，%v 后重试", rc.label, err, backoff)
			select {
			case <-time.After(backoff):
			case <-rc.ctx.Done():
				return
			}
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}

		backoff = 500 * time.Millisecond
		log.Printf("[%s] 连接建立成功", rc.label)

		rc.mu.Lock()
		rc.conn = conn
		rc.mu.Unlock()

		errCh := make(chan struct{}, 2)
		go rc.writeLoop(conn, errCh)
		go rc.readLoop(conn, errCh)

		select {
		case <-errCh:
		case <-rc.ctx.Done():
			conn.Close()
			return
		}

		conn.Close()
		log.Printf("[%s] 连接断开，%v 后重连", rc.label, backoff)
		select {
		case <-time.After(backoff):
		case <-rc.ctx.Done():
			return
		}
		backoff = minDuration(backoff*2, maxBackoff)
	}
}

func (rc *reconnConn) writeLoop(conn *websocket.Conn, errCh chan struct{}) {
	defer func() { errCh <- struct{}{} }()
	for {
		select {
		case data, ok := <-rc.ch:
			if !ok {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Printf("[%s] 写入失败: %v", rc.label, err)
				return
			}
		case <-rc.ctx.Done():
			return
		}
	}
}

func (rc *reconnConn) readLoop(conn *websocket.Conn, errCh chan struct{}) {
	defer func() { errCh <- struct{}{} }()
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if rc.ctx.Err() == nil {
				log.Printf("[%s] 读取失败: %v", rc.label, err)
			}
			return
		}
		if rc.onRecv != nil {
			rc.onRecv(raw)
		}
	}
}

// Send 阻塞投入发送队列，让发送速率随网络自然降速
func (rc *reconnConn) Send(data []byte) {
	select {
	case rc.ch <- data:
	case <-rc.ctx.Done():
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
