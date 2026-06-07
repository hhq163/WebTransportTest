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

// maxSentSeq 记录实际发出的最大序号，用于 FinalReport 统计范围
var maxSentSeq atomic.Int64

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

	// 创建流路由器，回包时记录 seq + RTT
	router := impl.NewStreamRouter(session, globalStats, func(msgType string, raw []byte, stats *impl.Stats) {
		var echo base.Message
		if json.Unmarshal(raw, &echo) != nil {
			return
		}
		rtt := base.RTTMs(echo.SendTimeMs)
		stats.RecordReceived(echo.Seq)
		stats.RecordRTT(rtt)
		log.Printf("[router:%s] 收到 seq=%d RTT=%dms", msgType, echo.Seq, rtt)
	})
	defer router.Close()

	for _, msgType := range []string{"ping", "chat", "game"} {
		if err := router.Open(msgType); err != nil {
			log.Fatalf("建立流失败 type=%s: %v", msgType, err)
		}
	}

	ctx, cancel := context.WithCancel(session.Context())
	defer cancel()

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

	// done 在所有 sendLoop 退出后关闭
	allSent := make(chan struct{})
	var sendersDone atomic.Int32
	onSenderDone := func() {
		if sendersDone.Add(1) == 3 {
			close(allSent)
		}
	}

	go receiveMessages(session, globalStats)

	go sendLoop(ctx, session, router, "ping", 100*time.Millisecond, globalStats, onSenderDone)
	go sendLoop(ctx, session, router, "chat", 100*time.Millisecond, globalStats, onSenderDone)
	go sendLoop(ctx, session, router, "game", 200*time.Millisecond, globalStats, onSenderDone)

	// 等待全部发完
	<-allSent
	totalSent := maxSentSeq.Load()
	log.Printf("全部 %d 条消息已发出，等待回包（5s grace period）...", totalSent)

	// grace period：等剩余回包
	time.Sleep(5 * time.Second)

	globalStats.FinalReport(totalSent)
	os.Exit(0)
}

// sendLoop 发送指定类型消息，达到全局 TotalSend 上限后退出
func sendLoop(ctx context.Context, session *webtransport.Session, router *impl.StreamRouter,
	msgType string, interval time.Duration, stats *impl.Stats, done func()) {
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
			for {
				cur := maxSentSeq.Load()
				if seq <= cur || maxSentSeq.CompareAndSwap(cur, seq) {
					break
				}
			}
			msg := &base.Message{
				Type:       msgType,
				Seq:        seq,
				Payload:    json.RawMessage(fmt.Sprintf(`{"text":%q}`, msgText)),
				SendTimeMs: time.Now().UnixMilli(),
			}
			if err := router.Send(msg); err != nil {
				log.Printf("[%s] 发送失败 seq=%d: %v", msgType, seq, err)
				stats.StreamFail.Add(1)
			} else {
				log.Printf("[%s] 发送 seq=%d", msgType, seq)
			}
		case <-ctx.Done():
			return
		}
	}
}

func receiveMessages(session *webtransport.Session, stats *impl.Stats) {
	for {
		stream, err := session.AcceptStream(context.Background())
		if err != nil {
			return
		}
		go func() {
			defer stream.Close()
			buf := make([]byte, 65536)
			for {
				n, err := stream.Read(buf)
				if err != nil {
					return
				}
				if n > 0 {
					log.Printf("[stream] 收到服务器推送: %s", string(buf[:n]))
				}
			}
		}()
	}
}

func sendDatagramLoop(session *webtransport.Session, stats *impl.Stats) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		seq := globalSeq.Add(1)
		if seq > impl.TotalSend {
			return
		}
		msg := &base.Message{
			Type:       "dgram",
			Seq:        seq,
			Payload:    json.RawMessage(fmt.Sprintf(`{"text":%q}`, msgText)),
			SendTimeMs: time.Now().UnixMilli(),
		}
		data, _ := json.Marshal(msg)
		if err := impl.SendDatagramWithRetry(session, data, stats, 3); err != nil {
			log.Printf("[datagram] 发送失败 seq=%d: %v", seq, err)
		}
	}
}

func receiveDatagrams(session *webtransport.Session, stats *impl.Stats) {
	for {
		data, err := session.ReceiveDatagram(context.Background())
		if err != nil {
			return
		}
		var msg base.Message
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		rtt := base.RTTMs(msg.SendTimeMs)
		stats.RecordReceived(msg.Seq)
		stats.RecordRTT(rtt)
		log.Printf("[datagram] 收到 seq=%d RTT=%dms", msg.Seq, rtt)
	}
}
