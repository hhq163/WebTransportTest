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

var globalSeq atomic.Int64
var globalStats = impl.NewStats()

var wsDialer = &websocket.Dialer{
	TLSClientConfig:  &tls.Config{InsecureSkipVerify: true},
	HandshakeTimeout: 10 * time.Second,
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

	onRecv := func(raw []byte) {
		var msg base.Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("[recv] 解析失败: %v", err)
			return
		}
		rtt := base.RTTMs(msg.SendTimeMs)
		globalStats.RecordRTT(rtt)
		globalStats.Received.Add(1)
		log.Printf("[recv] type=%s seq=%d RTT=%dms", msg.Type, msg.Seq, rtt)
	}

	// 连接 1：负责 chat（100ms）
	dialStart := time.Now()
	conn1 := newReconnConn(ctx, "conn1",
		func() (*websocket.Conn, error) {
			c, _, err := wsDialer.Dial(serverAddr+"/ws1", nil)
			if err == nil {
				log.Printf("[conn1] 连接建立成功 耗时=%dms", time.Since(dialStart).Milliseconds())
			}
			return c, err
		}, onRecv)

	// 连接 2：负责 game（200ms）
	dialStart = time.Now()
	conn2 := newReconnConn(ctx, "conn2",
		func() (*websocket.Conn, error) {
			c, _, err := wsDialer.Dial(serverAddr+"/ws2", nil)
			if err == nil {
				log.Printf("[conn2] 连接建立成功 耗时=%dms", time.Since(dialStart).Milliseconds())
			}
			return c, err
		}, onRecv)

	// 等首次连接建立
	time.Sleep(500 * time.Millisecond)

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

	globalStats.Start()

	allSent := make(chan struct{})
	var sendersDone atomic.Int32
	onSenderDone := func() {
		if sendersDone.Add(1) == 2 {
			close(allSent)
		}
	}

	go sendLoop(ctx, conn1, "chat", 10*time.Millisecond, onSenderDone)
	go sendLoop(ctx, conn2, "game", 20*time.Millisecond, onSenderDone)

	<-allSent
	globalStats.Stop()
	log.Printf("全部消息已发出，等待回包（5s grace period）...")
	time.Sleep(5 * time.Second)

	globalStats.FinalReport()
	os.Exit(0)
}

func sendLoop(ctx context.Context, conn *reconnConn, msgType string, interval time.Duration, done func()) {
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
			msg := base.NewMessage(msgType, seq, json.RawMessage(
				fmt.Sprintf(`{"text":%q}`, msgText),
			))
			data, _ := json.Marshal(msg)
			conn.Send(data)
			globalStats.Sent.Add(1)
			log.Printf("[%s] 发送 seq=%d", msgType, seq)
		case <-ctx.Done():
			return
		}
	}
}
