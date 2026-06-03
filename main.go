package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"log"
	"net/http"

	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
)

func main() {
	cert, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
	if err != nil {
		log.Fatal("加载证书失败:", err)
	}

	s := &webtransport.Server{
		H3: &http3.Server{
			Addr: ":4433",
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
			},
		},
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			return origin == "" || origin == "https://localhost" || origin == "https://127.0.0.1"
		},
	}

	http.HandleFunc("/wt", func(w http.ResponseWriter, r *http.Request) {
		session, err := s.Upgrade(w, r)
		if err != nil {
			log.Printf("升级失败: %v", err)
			return
		}

		log.Printf("新会话建立")
		defer session.CloseWithError(0, "")

		go handleStreams(session)
		go handleDatagrams(session)

		<-session.Context().Done()
		log.Printf("会话关闭")
	})

	log.Println("WebTransport 服务启动在 :4433")
	if err := s.ListenAndServeTLS("cert.pem", "key.pem"); err != nil {
		log.Fatal("服务启动失败:", err)
	}
}

// 处理双向流
func handleStreams(session *webtransport.Session) {
	for {
		stream, err := session.AcceptStream(context.Background())
		if err != nil {
			log.Printf("接受流失败: %v", err)
			return
		}

		go func() {
			defer stream.Close()

			buf := make([]byte, 1024)
			n, err := stream.Read(buf)
			if err != nil {
				log.Printf("读取失败: %v", err)
				return
			}

			msg := buf[:n]
			log.Printf("收到消息: %s", string(msg))

			response := bytes.ToUpper(msg)
			stream.Write(response)
			log.Printf("回复: %s", string(response))
		}()
	}
}

// 处理不可靠数据报（适合实时位置同步等场景）
func handleDatagrams(session *webtransport.Session) {
	for {
		data, err := session.ReceiveDatagram(context.Background())
		if err != nil {
			return
		}
		log.Printf("收到数据报: %d 字节, 内容: %s", len(data), string(data))

		// 回显
		session.SendDatagram([]byte("Server Echo:" + string(data)))

	}
}
