package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/hhq163/WebTransportTest/client/impl"
	"github.com/quic-go/quic-go"
	webtransport "github.com/quic-go/webtransport-go"
)

const msgText = "But apart from our regular occupation how much are we alive? If you are interested only in your regular occupation, you are alive only to that extent."

var globalStats impl.Stats

func main() {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h3"},
	}

	dialer := &webtransport.Dialer{
		TLSClientConfig: tlsConfig,
		QUICConfig: &quic.Config{
			EnableDatagrams:                  true,
			EnableStreamResetPartialDelivery: true,
			MaxIdleTimeout:                   6 * time.Second,
			KeepAlivePeriod:                  2 * time.Second,
		},
	}
	defer dialer.Close()

	resp, session, err := dialer.Dial(context.Background(), "https://172.16.121.61:4433/wt", http.Header{
		"User-Agent": []string{"Go-WebTransport-Client/1.0"},
	})
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer session.CloseWithError(0, "")

	if resp != nil && resp.StatusCode != 200 {
		log.Printf("警告: 服务端返回非 200 状态码: %d %s", resp.StatusCode, resp.Status)
	}
	log.Printf("成功连接到服务端")

	// 创建流路由器：按消息类型分配固定流，响应回调记录 RTT
	router := impl.NewStreamRouter(session, &globalStats, func(msgType string, resp []byte, stats *impl.Stats) {
		rttMs := int64(-1)
		var echo impl.Message
		if json.Unmarshal(resp, &echo) == nil && echo.SendTimeMs > 0 {
			rttMs = impl.RTTMs(echo.SendTimeMs)
			stats.RecordRTT(rttMs)
		}
		log.Printf("[router:%s] 收到响应 (%d 字节) RTT=%dms", msgType, len(resp), rttMs)
	})
	defer router.Close()

	// 预建立各类型专用流
	for _, msgType := range []string{"ping", "chat", "game"} {
		if err := router.Open(msgType); err != nil {
			log.Fatalf("建立流失败 type=%s: %v", msgType, err)
		}
	}

	// 定时打印统计
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				log.Printf("[stats] %s", globalStats.String())
			case <-session.Context().Done():
				return
			}
		}
	}()

	// 各类型流的发送循环
	go sendLoop(session, router, "ping", 1*time.Second, &globalStats)
	go sendLoop(session, router, "chat", 2*time.Second, &globalStats)
	go sendLoop(session, router, "game", 500*time.Millisecond, &globalStats)

	// 数据报发送循环（无需流，适合不可靠实时数据）
	// go sendDatagramLoop(session, &globalStats)

	// 接收服务端主动推送的流
	go receiveMessages(session, &globalStats)
	// go receiveDatagrams(session, &globalStats)

	<-session.Context().Done()
	log.Printf("连接断开 | 最终统计: %s", globalStats.String())
}

// sendLoop 按 interval 周期通过 router 发送指定类型的消息
func sendLoop(session *webtransport.Session, router *impl.StreamRouter, msgType string, interval time.Duration, stats *impl.Stats) {
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
			if err := router.Send(msg); err != nil {
				log.Printf("[%s] 发送失败 seq=%d: %v", msgType, seq, err)
				stats.StreamFail.Add(1)
			} else {
				log.Printf("[%s] 发送 seq=%d", msgType, seq)
			}
		case <-session.Context().Done():
			return
		}
	}
}

// sendDatagramLoop 每 500ms 发送一条不可靠数据报
func sendDatagramLoop(session *webtransport.Session, stats *impl.Stats) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	seq := 0
	for {
		select {
		case <-ticker.C:
			seq++
			msg := impl.NewMessage("dgram", json.RawMessage(
				fmt.Sprintf(`{"seq":%d,"text":%q}`, seq, msgText),
			))
			data, _ := json.Marshal(msg)
			if err := impl.SendDatagramWithRetry(session, data, stats, 3); err != nil {
				log.Printf("[datagram] 发送失败 seq=%d: %v", seq, err)
			} else {
				log.Printf("[datagram] 发送 seq=%d", seq)
			}
		case <-session.Context().Done():
			return
		}
	}
}

// receiveMessages 接收服务端主动推送的流（非 router 管理的流）
func receiveMessages(session *webtransport.Session, stats *impl.Stats) {
	for {
		stream, err := session.AcceptStream(context.Background())
		if err != nil {
			log.Printf("接收流失败: %v", err)
			return
		}
		go handleStream(stream, stats)
	}
}

func receiveDatagrams(session *webtransport.Session, stats *impl.Stats) {
	for {
		data, err := session.ReceiveDatagram(context.Background())
		if err != nil {
			log.Printf("[datagram] 接收失败: %v", err)
			return
		}
		var msg impl.Message
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("[datagram] 解析失败: %v", err)
			continue
		}
		rtt := impl.RTTMs(msg.SendTimeMs)
		stats.RecordReceived()
		stats.RecordRTT(rtt)
		log.Printf("[datagram] 收到 type=%s RTT=%dms", msg.Type, rtt)
	}
}

func handleStream(stream *webtransport.Stream, stats *impl.Stats) {
	defer stream.Close()
	buf := make([]byte, 65536)
	for {
		n, err := stream.Read(buf)
		if err != nil {
			if err.Error() != "EOF" {
				log.Printf("[stream] 读取错误: %v", err)
			}
			return
		}
		if n > 0 {
			stats.RecordReceived()
			log.Printf("[stream] 收到服务器推送: %s", string(buf[:n]))
			stream.Write([]byte(`{"status":"ok"}`))
		}
	}
}
