package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
)

type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func main() {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h3"},
	}

	dialer := &webtransport.Dialer{
		TLSClientConfig: tlsConfig,
		// 可选：附加请求头
	}
	defer dialer.Close()

	// Dial 返回 (*http.Response, *Session, error)
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

	go receiveMessages(session)

	sendTestMessages(session)

	<-make(chan struct{})
}

func sendTestMessages(session *webtransport.Session) {
	time.Sleep(500 * time.Millisecond)

	// OpenStream 不再接受 context 参数
	stream, err := session.OpenStream()
	if err != nil {
		log.Printf("打开流失败: %v", err)
		return
	}
	defer stream.Close()

	msg := Message{
		Type:    "ping",
		Payload: json.RawMessage(`{"timestamp":` + fmt.Sprintf("%d", time.Now().Unix()) + `}`),
	}
	data, _ := json.Marshal(msg)

	n, err := stream.Write(data)
	if err != nil {
		log.Printf("发送失败: %v", err)
		return
	}
	log.Printf("发送消息 (%d 字节): %s", n, string(data))

	buf := make([]byte, 4096)
	n, err = stream.Read(buf)
	if err != nil {
		log.Printf("读取响应失败: %v", err)
		return
	}
	log.Printf("收到响应: %s", string(buf[:n]))
}

func receiveMessages(session *webtransport.Session) {
	for {
		stream, err := session.AcceptStream(context.Background())
		if err != nil {
			log.Printf("接收流失败: %v", err)
			return
		}
		go handleStream(stream)
	}
}

// AcceptStream 返回 *webtransport.Stream，参数类型与之匹配
func handleStream(stream *webtransport.Stream) {
	defer stream.Close()

	buf := make([]byte, 65536)
	for {
		n, err := stream.Read(buf)
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("流正常关闭")
			} else {
				log.Printf("读取流错误: %v", err)
			}
			return
		}

		if n > 0 {
			log.Printf("收到服务器推送: %s", string(buf[:n]))

			ack := []byte(`{"status":"ok"}`)
			stream.Write(ack)
		}
	}
}
