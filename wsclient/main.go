package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hhq163/WebTransportTest/base"
	"github.com/hhq163/WebTransportTest/wsclient/impl"
)

const (
	serverAddr = "wss://172.16.121.61:9001"
	msgText    = "But apart from our regular occupation how much are we alive? If you are interested only in your regular occupation, you are alive only to that extent."
)

// globalSeq 跨所有连接共享的全局消息序号
var globalSeq atomic.Int64

// maxSentSeq 记录实际发出的最大序号，用于 FinalReport 统计范围
var maxSentSeq atomic.Int64

var globalStats = impl.NewStats()

// writerConn 通过单一 writer goroutine + channel 保证写安全
type writerConn struct {
	conn *websocket.Conn
	ch   chan []byte
}

var wsDialer = &websocket.Dialer{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

func newWriterConn(c *websocket.Conn, ctx context.Context) *writerConn {
	wc := &writerConn{conn: c, ch: make(chan []byte, 512)}
	go wc.writeLoop(ctx)
	return wc
}

func (wc *writerConn) writeLoop(ctx context.Context) {
	for {
		select {
		case data, ok := <-wc.ch:
			if !ok {
				return
			}
			if err := wc.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Printf("[writerConn] 写入失败: %v", err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

// Send 阻塞投入发送队列，让发送速率随网络自然降速，不丢弃消息
func (wc *writerConn) Send(data []byte, ctx context.Context) {
	select {
	case wc.ch <- data:
	case <-ctx.Done():
	}
}

func (wc *writerConn) ReadMessage() (int, []byte, error) { return wc.conn.ReadMessage() }
func (wc *writerConn) Close() error                      { return wc.conn.Close() }
func (wc *writerConn) SetPongHandler(h func(string) error) {
	wc.conn.SetPongHandler(h)
}

func initLogger() *os.File {
	logFile, err := os.OpenFile(
		fmt.Sprintf("ws_client_%s.log", time.Now().Format("20060102_150405")),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644,
	)
	if err != nil {
		log.Fatalf("无法创建日志文件: %v", err)
	}
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	return logFile
}

func main() {
	logFile := initLogger()
	defer logFile.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 连接 1：负责 ping（100ms）+ chat（100ms）
	dialStart := time.Now()
	rawConn1, _, err := wsDialer.Dial(serverAddr+"/ws1", nil)
	if err != nil {
		log.Fatalf("[conn1] 连接失败: %v", err)
	}
	conn1 := newWriterConn(rawConn1, ctx)
	defer conn1.Close()
	log.Printf("[conn1] 连接建立成功 /ws1 耗时=%dms", time.Since(dialStart).Milliseconds())

	// 连接 2：负责 game（200ms）
	dialStart = time.Now()
	rawConn2, _, err := wsDialer.Dial(serverAddr+"/ws2", nil)
	if err != nil {
		log.Fatalf("[conn2] 连接失败: %v", err)
	}
	conn2 := newWriterConn(rawConn2, ctx)
	defer conn2.Close()
	log.Printf("[conn2] 连接建立成功 /ws2 耗时=%dms", time.Since(dialStart).Milliseconds())

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

	go receiveLoop(ctx, conn1, "conn1", globalStats)
	go receiveLoop(ctx, conn2, "conn2", globalStats)

	// allSent 在所有 sendLoop 退出后关闭
	allSent := make(chan struct{})
	var sendersDone atomic.Int32
	onSenderDone := func() {
		if sendersDone.Add(1) == 3 {
			close(allSent)
		}
	}

	go sendLoop(ctx, conn1, "ping", 100*time.Millisecond,
		&globalStats.Conn1Sent, onSenderDone)
	go sendLoop(ctx, conn1, "chat", 100*time.Millisecond,
		&globalStats.Conn1Sent, onSenderDone)
	go sendLoop(ctx, conn2, "game", 200*time.Millisecond,
		&globalStats.Conn2Sent, onSenderDone)

	<-allSent
	totalSent := maxSentSeq.Load()
	log.Printf("全部 %d 条消息已发出，等待回包（5s grace period）...", totalSent)
	time.Sleep(5 * time.Second)

	globalStats.FinalReport(totalSent)
	os.Exit(0)
}

// sendLoop 发满 TotalSend 条后退出
func sendLoop(ctx context.Context, conn *writerConn, msgType string, interval time.Duration,
	sent *atomic.Int64, done func()) {
	defer done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			seq := globalSeq.Add(1)
			if seq > impl.TotalSend {
				return
			}
			// 记录实际发出的最大序号
			for {
				cur := maxSentSeq.Load()
				if seq <= cur || maxSentSeq.CompareAndSwap(cur, seq) {
					break
				}
			}
			msg := base.NewMessage(msgType, seq, json.RawMessage(
				fmt.Sprintf(`{"text":%q}`, msgText),
			))
			data, _ := json.Marshal(msg)
			conn.Send(data, ctx)
			sent.Add(1)
			log.Printf("[%s] 发送 seq=%d", msgType, seq)
		case <-ctx.Done():
			return
		}
	}
}

// receiveLoop 持续接收回包，解析 seq 和 RTT
func receiveLoop(ctx context.Context, conn *writerConn, label string, stats *impl.Stats) {
	conn.SetPongHandler(func(string) error { return nil })
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("[%s] 读取失败: %v", label, err)
			}
			return
		}
		var msg base.Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("[%s] 解析失败: %v", label, err)
			continue
		}
		rtt := base.RTTMs(msg.SendTimeMs)
		stats.RecordReceived(msg.Seq)
		stats.RecordRTT(rtt)
		log.Printf("[%s] 收到 seq=%d type=%s RTT=%dms", label, msg.Seq, msg.Type, rtt)
	}
}
