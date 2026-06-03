# WebTransportTest
WebTransport协议测试，WebSocket over QUIC


# 1.安装 mkcert
brew install mkcert          # macOS
# 或 sudo apt install mkcert # Linux

# 初始化本地 CA
mkcert -install

# 生成证书（针对 localhost 和你的内网 IP）
mkcert -key-file key.pem -cert-file cert.pem localhost 127.0.0.1 192.168.x.x

