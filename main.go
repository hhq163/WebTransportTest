package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/hhq163/WebTransportTest/impl"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
)

var globalStats impl.Stats

func main() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatal("加载证书失败:", err)
	}

	h3Server := &http3.Server{
		Addr: ":4433",
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h3"},
		},
	}
	// ConfigureHTTP3Server 注入三项必要配置：
	//   AdditionalSettings[SETTINGS_ENABLE_WEBTRANSPORT]=1
	//   EnableDatagrams=true
	//   ConnContext 注入 QUIC 连接（Upgrade 依赖）
	webtransport.ConfigureHTTP3Server(h3Server)

	s := &webtransport.Server{
		H3: h3Server,
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			return origin == "" || origin == "https://localhost" || origin == "https://127.0.0.1"
		},
	}

	// 独立 TCP HTTP Server，提供 stats 查询接口，可直接用浏览器访问
	// 访问方式：http://172.16.121.61:8080/stats
	statsMux := http.NewServeMux()
	statsMux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(globalStats.Snapshot())
	})
	go func() {
		log.Println("Stats HTTP 服务启动在 :8082  →  http://172.16.121.61:8082/stats")
		if err := http.ListenAndServe(":8082", statsMux); err != nil {
			log.Printf("Stats 服务异常: %v", err)
		}
	}()

	http.HandleFunc("/wt", func(w http.ResponseWriter, r *http.Request) {
		session, err := s.Upgrade(w, r)
		if err != nil {
			log.Printf("升级失败: %v", err)
			return
		}
		log.Printf("新会话建立")
		defer session.CloseWithError(0, "")

		ctx, cancel := context.WithCancel(session.Context())
		defer cancel()
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

		go handleStreams(session, &globalStats)
		go handleDatagrams(session, &globalStats)

		<-session.Context().Done()
		log.Printf("会话关闭 | 最终统计: %s", globalStats.String())
	})

	log.Println("WebTransport 服务启动在 :4433")
	if err := s.ListenAndServeTLS("cert.pem", "key.pem"); err != nil {
		log.Fatal("服务启动失败:", err)
	}
}

func handleStreams(session *webtransport.Session, stats *impl.Stats) {
	for {
		stream, err := session.AcceptStream(context.Background())
		if err != nil {
			log.Printf("接受流失败: %v", err)
			return
		}
		// 每条流独立 goroutine，循环读取该流上的所有消息
		go processStream(stream, stats)
	}
}

// processStream 持续读取同一条流上的消息，按 msg.Type 分派处理
func processStream(stream *webtransport.Stream, stats *impl.Stats) {
	defer stream.Close()
	buf := make([]byte, 65536)
	for {
		n, err := stream.Read(buf)
		if err != nil {
			if err.Error() != "EOF" {
				log.Printf("[stream] 读取失败: %v", err)
			}
			return
		}
		if n == 0 {
			continue
		}

		raw := make([]byte, n)
		copy(raw, buf[:n])
		stats.MsgReceived.Add(1)

		var msg impl.Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("[stream] 解析失败: %v, raw=%s", err, string(raw))
			continue
		}

		delay := msg.OneWayDelayMs()
		stats.RecordDelay(delay)

		switch msg.Type {
		case "ping":
			dispatchPing(stream, raw, stats)
		case "chat":
			dispatchChat(stream, msg, stats)
		case "game":
			dispatchGame(stream, msg, stats)
		default:
			log.Printf("[stream:%s] 收到", msg.Type)
			response := bytes.ToUpper(raw)
			if err := impl.WriteWithRetry(stream, response, stats, 3); err != nil {
				log.Printf("[stream:%s] 回包失败: %v", msg.Type, err)
			}
		}
	}
}

func dispatchPing(stream *webtransport.Stream, raw []byte, stats *impl.Stats) {
	log.Printf("[ping] 收到")
	if err := impl.WriteWithRetry(stream, raw, stats, 3); err != nil {
		log.Printf("[ping] 回包失败: %v", err)
	}
}

func dispatchChat(stream *webtransport.Stream, msg impl.Message, stats *impl.Stats) {
	log.Printf("[chat] 收到 text_len=%d", len(msg.Payload))
	reply, _ := json.Marshal(impl.Message{
		Type:       "chat_ack",
		Payload:    msg.Payload,
		SendTimeMs: msg.SendTimeMs,
	})
	if err := impl.WriteWithRetry(stream, reply, stats, 3); err != nil {
		log.Printf("[chat] 回包失败: %v", err)
	}
}

func dispatchGame(stream *webtransport.Stream, msg impl.Message, stats *impl.Stats) {
	log.Printf("[game] 收到")
	reply, _ := json.Marshal(impl.Message{
		Type:       "game_ack",
		Payload:    msg.Payload,
		SendTimeMs: msg.SendTimeMs,
	})
	if err := impl.WriteWithRetry(stream, reply, stats, 3); err != nil {
		log.Printf("[game] 回包失败: %v", err)
	}
}

func handleDatagrams(session *webtransport.Session, stats *impl.Stats) {
	for {
		data, err := session.ReceiveDatagram(context.Background())
		if err != nil {
			return
		}
		stats.MsgReceived.Add(1)

		var msg impl.Message
		if err := json.Unmarshal(data, &msg); err != nil {
			log.Printf("[datagram] 解析失败: %v", err)
			continue
		}
		log.Printf("[datagram] 收到 type=%s len=%d", msg.Type, len(data))

		reply, _ := json.Marshal(impl.Message{
			Type:       msg.Type + "_ack",
			Payload:    msg.Payload,
			SendTimeMs: msg.SendTimeMs,
		})
		if err := impl.SendDatagramWithRetry(session, reply, stats, 3); err != nil {
			log.Printf("[datagram] 最终发送失败: %v", err)
		}
	}
}
