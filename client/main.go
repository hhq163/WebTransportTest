package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/hhq163/WebTransportTest/base"
	"github.com/hhq163/WebTransportTest/client/impl"
	"github.com/quic-go/quic-go"
	webtransport "github.com/quic-go/webtransport-go"
)

const msgText = "But apart from our regular occupation how much are we alive? If you are interested only in your regular occupation, you are alive only to that extent."

// globalSeq 跨所有流共享的全局消息序号
var globalSeq atomic.Int64

var globalStats = impl.NewStats()

func initLogger() *os.File {
	logFile, err := os.OpenFile(
		fmt.Sprintf("wt_client_%s.log", time.Now().Format("20060102_150405")),
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

	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h3"},
	}

	dialer := &webtransport.Dialer{
		TLSClientConfig: tlsConfig,
		QUICConfig: &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
			MaxIdleTimeout:                   30 * time.Second,
			KeepAlivePeriod:                  2 * time.Second,
		},
	}
	defer dialer.Close()

	dialStart := time.Now()
	resp, session, err := dialer.Dial(context.Background(), "https://172.16.121.61:4433/wt", http.Header{
		"User-Agent": []string{"Go-WebTransport-Client/1.0"},
	})
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer session.CloseWithError(0, "")
	log.Printf("WebTransport 连接建立成功，耗时=%dms", time.Since(dialStart).Milliseconds())

	if resp != nil && resp.StatusCode != 200 {
		log.Printf("警告: 服务端返回非 200 状态码: %d %s", resp.StatusCode, resp.Status)
	}

	// 创建流路由器，回包时记录 RTT
	router := impl.NewStreamRouter(session, globalStats, func(msgType string, raw []byte, stats *impl.Stats) {
		var echo base.Message
		if json.Unmarshal(raw, &echo) != nil {
			return
		}
		rtt := base.RTTMs(echo.SendTimeMs)
		stats.RecordRTT(rtt)
		stats.Received.Add(1)
		log.Printf("[router:%s] 收到 seq=%d RTT=%dms", msgType, echo.Seq, rtt)
	})
	defer router.Close()

	// 只建立 chat 和 game 两种流
	for _, msgType := range []string{"chat", "game"} {
		if err := router.Open(msgType); err != nil {
			log.Fatalf("建立流失败 type=%s: %v", msgType, err)
		}
	}

	ctx, cancel := context.WithCancel(session.Context())
	defer cancel()

	// done 在所有发送循环完成后关闭
	allSent := make(chan struct{})
	var sendersDone atomic.Int32
	doneFunc := func() {
		if sendersDone.Add(1) == 2 { // 只有 chat 和 game 两种类型
			close(allSent)
		}
	}

	globalStats.Start()

	// 启动接收循环
	go receiveMessages(session)

	// 启动两种消息的发送循环
	go sendLoop(ctx, router, "chat", 10*time.Millisecond, globalStats, doneFunc)
	go sendLoop(ctx, router, "game", 20*time.Millisecond, globalStats, doneFunc)

	// 等待所有发送完成
	<-allSent
	globalStats.Stop()

	// 等待剩余响应到达
	time.Sleep(5 * time.Second)

	globalStats.FinalReport()
}

// sendLoop 发送循环，定期发送指定类型的消息
func sendLoop(ctx context.Context, router *impl.StreamRouter, msgType string, interval time.Duration, stats *impl.Stats, done func()) {
	defer done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			seq := globalSeq.Add(1)
			if seq > impl.TotalSend {
				return
			}
			msg := base.NewMessage(msgType, seq, json.RawMessage(
				fmt.Sprintf(`{"text":%q}`, msgText),
			))

			if err := router.Send(msg); err != nil {
				log.Printf("[%s] Send failed: %v", msgType, err)
				return
			}
			stats.Sent.Add(1)
			log.Printf("[%s] 发送 seq=%d", msgType, seq)
		}
	}
}

// receiveMessages 接收服务器推送的消息
func receiveMessages(session *webtransport.Session) {
	for {
		stream, err := session.AcceptStream(context.Background())
		if err != nil {
			log.Printf("AcceptStream failed: %v", err)
			return
		}
		go handleStream(stream)
	}
}

// handleStream 处理单个流的消息
func handleStream(stream *webtransport.Stream) {
	defer stream.Close()
	for {
		msg, err := base.ReadMessage(stream)
		if err != nil {
			return
		}
		log.Printf("[stream] 收到服务器推送: type=%s seq=%d", msg.Type, msg.Seq)
	}
}
