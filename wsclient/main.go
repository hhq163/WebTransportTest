package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hhq163/WebTransportTest/wsclient/impl"
)

const (
	serverAddr = "ws://172.16.121.61:9001"
	msgText    = "But apart from our regular occupation how much are we alive? If you are interested only in your regular occupation, you are alive only to that extent."
)

var globalStats impl.Stats

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 连接 1：负责 ping（1s）+ chat（2s）
	conn1, _, err := websocket.DefaultDialer.Dial(serverAddr+"/ws1", nil)
	if err != nil {
		log.Fatalf("[conn1] 连接失败: %v", err)
	}
	defer conn1.Close()
	log.Println("[conn1] 连接成功 /ws1")

	// 连接 2：负责 game（500ms）
	conn2, _, err := websocket.DefaultDialer.Dial(serverAddr+"/ws2", nil)
	if err != nil {
		log.Fatalf("[conn2] 连接失败: %v", err)
	}
	defer conn2.Close()
	log.Println("[conn2] 连接成功 /ws2")

	// 定时打印统计
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf("[stats] %s", globalStats.String())
			case <-ctx.Done():
				return
			}
		}
	}()

	// 接收 conn1 回包
	go receiveLoop(ctx, conn1, "conn1", &globalStats)
	// 接收 conn2 回包
	go receiveLoop(ctx, conn2, "conn2", &globalStats)

	// 发送循环
	go sendLoop(ctx, conn1, "ping", 1*time.Second,
		&globalStats.Conn1Sent, &globalStats.Conn1Fail, &globalStats.Conn1Retry)
	go sendLoop(ctx, conn1, "chat", 2*time.Second,
		&globalStats.Conn1Sent, &globalStats.Conn1Fail, &globalStats.Conn1Retry)
	go sendLoop(ctx, conn2, "game", 500*time.Millisecond,
		&globalStats.Conn2Sent, &globalStats.Conn2Fail, &globalStats.Conn2Retry)

	// 等待任意连接断开则退出
	done := make(chan struct{})
	go func() {
		// conn1/conn2 断开后 receiveLoop 会退出，通过 cancel 通知主 goroutine
		<-ctx.Done()
		close(done)
	}()
	<-done
	log.Printf("连接断开 | 最终统计: %s", globalStats.String())
}

// sendLoop 以固定间隔通过指定连接发送消息，带重试
func sendLoop(ctx context.Context, conn *websocket.Conn, msgType string, interval time.Duration,
	sent, fail, retry *atomic.Int64) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	seq := 0
	for {
		select {
		case <-ticker.C:
			seq++
			msg := impl.NewMessage(msgType, json.RawMessage(
				fmt.Sprintf(`{"seq":%d,"text":%q}`, seq, msgText),
			))
			data, _ := json.Marshal(msg)
			err := impl.WriteWithRetry(func(b []byte) error {
				return conn.WriteMessage(websocket.TextMessage, b)
			}, data, sent, fail, retry, 3)
			if err != nil {
				log.Printf("[%s] 发送失败 seq=%d: %v", msgType, seq, err)
			} else {
				log.Printf("[%s] 发送 seq=%d", msgType, seq)
			}
		case <-ctx.Done():
			return
		}
	}
}

// receiveLoop 持续接收回包，计算 RTT 并更新统计
func receiveLoop(ctx context.Context, conn *websocket.Conn, label string, stats *impl.Stats) {
	conn.SetPongHandler(func(string) error { return nil })
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("[%s] 读取失败: %v", label, err)
			}
			return
		}

		var msg impl.Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("[%s] 解析失败: %v", label, err)
			continue
		}

		rtt := impl.RTTMs(msg.SendTimeMs)
		stats.RecordReceived()
		stats.RecordRTT(rtt)
		log.Printf("[%s] 收到 type=%s RTT=%dms", label, msg.Type, rtt)
	}
}
