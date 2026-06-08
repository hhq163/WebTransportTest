# 使用WebSocket over QUIC改善弱网下游戏体验可行性

WebSocket over QUIC业界更标准的称呼是WebTransport，以下都用WebTransport
## WebTransport介绍
WebTransport 解决了 WebSocket 最令人头疼的两个问题：队头阻塞和连接建立慢，并新增了对不可靠传输（如实时音视频）的支持；
QUIC (基于 UDP)，多路流复用，可同时包含多个有序流和无序数据报；可靠传输队头阻塞问题的处理，丢包仅影响其所在的独立流，不影响其它流；支持流 (可靠、有序) 和数据报 (不可靠、无序) 两种传输模式；QUIC 自身集成了 TLS，减少了握手往返次数，连接建立更快；

QUIC数据报模式和UDP数据报的区别：QUIC数据报消息是加密过的，必须在QUIC连接建立后才能发送，连接是安全可靠的，有拥塞控制（与可靠流共享）；

## 可行性
可行性很高，系统变动如下：
1. 服务端及客户端实现：
服务端：当前使用websocket协议，服务端使用Nginx支持WSS+网关服（实现ws协议解析，和nginx使用TCP连接）支持WSS，如果换成WebTransport，有多种方案可选：

方案一（推荐）：用Nginx做UDP反向代理，将UDP消息转发给后面的网关服，网关服实现webTransport的支持（quic+http3）,有成熟的开源库：https://github.com/litespeedtech/lsquic
方案二：让Nginx 支持 WebTransport， 和网关使用websocket连接，Nginx要做协议转换，会产生额外的延迟，同时失去不可靠传输 (Datagram)能力

客户端：C#目前没有官方内置的 WebTransport 客户端 API，需要基于lsquic 的 client 能力进行封装；

2. 消息路由及安全性上支持：nginx开启UDP代理，阿里云DDOS高防配置中开启 UDP 反射攻击防护功能即可

3. 潜在风险：因运营商的NAT和限流策略，NAT穿透性比TCP差，UDP常被限速或限制，跨运营商（如电信到联通）或网络使用高峰期，UDP丢包率和延迟会增加,须保留WebSocket作为自动降级方案。


## 双TCP方案和WebTransport对比测试；
1. 测试方案：
- 双TCP方案：客户端和服务器之间建立两个TCP连接，一个用于游戏数据，另一个用于心跳和控制消息。



| 编号      | 丢包率 | 附加延迟 | 说明          |       双WSS   |       WebTransport    |
| ----------- | ----------- | -----------     | ----------- | ----------- | ----------- |   
| 1   | 0%  | 0ms      | 无丢包无延迟     | RTT avg=0.9ms p95=5ms max=7ms 第2000条耗时=79900ms  | RTT avg=1.2ms p95=5ms max=12ms 第2000条耗时=79898ms |
| 2   | 1%  | 0ms      | 轻微丢包         | RTT avg=0.9ms p95=5ms max=12ms 第2000条耗时=79898ms |RTT avg=1.2ms p95=5ms max=5ms 第2000条耗时=79901ms  |
| 3   | 5%  | 0ms      | 中等丢包         | RTT avg=0.9ms p95=5ms max=12ms 第2000条耗时=79898ms  | RTT avg=1.1ms p95=5ms max=4ms 第2000条耗时=79899ms |
| 4   | 10% | 0ms     | 较重丢包（HOL 验证）| RTT avg=0.9ms p95=5ms max=19ms 第2000条耗时=79900ms  | RTT avg=1.2ms p95=5ms max=13ms 第2000条耗时=79898ms |
| 5   | 20% | 0ms     | 极端丢包         | RTT avg=0.9ms p95=5ms max=13ms 第2000条耗时=79898ms | RTT avg=1.2ms p95=5ms max=9ms  第2000条耗时=79899ms| 


发送 2000 条 → 统计实际收到了多少条（到达率 = recv / sent）

| 编号      | 丢包率 | 附加延迟 | 说明          |       双WSS   |       WebTransport    |
| ----------- | ----------- | -----------     | ----------- | ----------- | ----------- |   
| 1   | 20% | 0ms     | 较重丢包（HOL 验证）| 到达率=100.0%  RTT avg=414.3ms p95=1855ms max=9346ms | 到达率=99.8%  RTT avg=8.1ms p95=35ms max=86ms|
| 2   | 25%  | 0ms      | 较重丢包         |  到达率=100.0%  RTT avg=2307.2ms p95=9415ms max=12577ms | 到达率=98.8%  RTT avg=10.7ms p95=35ms max=201ms |
| 3   | 30% | 0ms     | 极端丢包         | 到达率=42.8%  RTT avg=8648.1ms p95=9995ms max=33912ms| 到达率=97.3%  RTT avg=12.9ms p95=45ms max=186ms| 
| 4   | 35% | 0ms     | 极端丢包         | 到达率=61.5%  RTT avg=4071.2ms p95=9995ms max=34752ms|  到达率=95.3%  RTT avg=16.7ms p95=85ms max=232ms|
| 5   |40% | 0ms     | 极端丢包         | 到达率=61.5%  RTT avg=1838.4ms p95=7215ms max=20715ms | 到达率=92.3%  RTT avg=18.7ms p95=85ms max=503ms |
| 6   |50% | 0ms     | 极端丢包         | 到达率=40.2%  RTT avg=79330.3ms p95=9995ms max=159865ms | 到达率=79.0%  RTT avg=31.5ms p95=95ms max=1012ms | 