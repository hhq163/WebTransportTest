package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hhq163/WebTransportTest/base"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// 全局统计（服务端只统计收发计数，RTT 由客户端计算）
var (
	totalReceived atomic.Int64
	totalSent     atomic.Int64
	totalFail     atomic.Int64
)

// sessionMgr 记录活跃连接数
var (
	connMu    sync.Mutex
	connCount int
)

func main() {
	mux := http.NewServeMux()

	// /ws1 - 连接1：处理 ping + chat 消息
	mux.HandleFunc("/ws1", func(w http.ResponseWriter, r *http.Request) {
		handleConn(w, r, "ws1")
	})

	// /ws2 - 连接2：处理 game 消息
	mux.HandleFunc("/ws2", func(w http.ResponseWriter, r *http.Request) {
		handleConn(w, r, "ws2")
	})

	// /stats - 服务端统计
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		connMu.Lock()
		cc := connCount
		connMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"total_received": totalReceived.Load(),
			"total_sent":     totalSent.Load(),
			"total_fail":     totalFail.Load(),
			"active_conns":   cc,
		})
	})

	log.Println("WsServer (WSS) 启动在 :9001  /ws1=ping+chat  /ws2=game  /stats=统计")
	if err := http.ListenAndServeTLS(":9001", "cert.pem", "key.pem", mux); err != nil {
		log.Fatal(err)
	}
}

func handleConn(w http.ResponseWriter, r *http.Request, label string) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[%s] upgrade 失败: %v", label, err)
		return
	}
	defer conn.Close()

	connMu.Lock()
	connCount++
	log.Printf("[%s] 连接建立，当前活跃=%d", label, connCount)
	connMu.Unlock()

	defer func() {
		connMu.Lock()
		connCount--
		log.Printf("[%s] 连接断开，当前活跃=%d", label, connCount)
		connMu.Unlock()
	}()

	conn.SetReadDeadline(time.Time{}) // 不设超时，依赖客户端 ping
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		return nil
	})

	// 定时发 Ping 保活
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(3*time.Second)); err != nil {
				return
			}
		}
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				log.Printf("[%s] 读取失败: %v", label, err)
			}
			return
		}
		totalReceived.Add(1)

		var msg base.Message
		if err := json.Unmarshal(raw, &msg); err != nil {
			log.Printf("[%s] 解析失败: %v", label, err)
			continue
		}
		log.Printf("[%s] 收到 type=%s seq=%d", label, msg.Type, msg.Seq)

		// 直接回显原始消息，客户端通过 send_time_ms 算 RTT、seq 算到达率
		if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
			log.Printf("[%s] 回包失败: %v", label, err)
			totalFail.Add(1)
			return
		}
		totalSent.Add(1)
	}
}
