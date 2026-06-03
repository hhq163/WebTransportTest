package impl

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
)

// Stats 记录客户端消息收发及 RTT 统计，所有字段均线程安全
type Stats struct {
	StreamSent    atomic.Int64
	StreamFail    atomic.Int64
	StreamRetry   atomic.Int64
	DatagramSent  atomic.Int64
	DatagramFail  atomic.Int64
	DatagramRetry atomic.Int64
	MsgReceived   atomic.Int64

	// RTT 统计（毫秒）
	RTTTotalMs atomic.Int64
	RTTCount   atomic.Int64
	RTTMaxMs   atomic.Int64
}

// RecordRTT 记录一次往返延迟（毫秒），rttMs < 0 表示无效跳过
func (s *Stats) RecordRTT(rttMs int64) {
	if rttMs < 0 {
		return
	}
	s.RTTTotalMs.Add(rttMs)
	s.RTTCount.Add(1)
	for {
		cur := s.RTTMaxMs.Load()
		if rttMs <= cur {
			break
		}
		if s.RTTMaxMs.CompareAndSwap(cur, rttMs) {
			break
		}
	}
}

// AvgRTTMs 返回平均 RTT（毫秒），无样本时返回 -1
func (s *Stats) AvgRTTMs() float64 {
	cnt := s.RTTCount.Load()
	if cnt == 0 {
		return -1
	}
	return float64(s.RTTTotalMs.Load()) / float64(cnt)
}

func (s *Stats) String() string {
	return fmt.Sprintf(
		"[Stream] sent=%d fail=%d retry=%d | [Datagram] sent=%d fail=%d retry=%d | recv=%d | RTT avg=%.1fms max=%dms",
		s.StreamSent.Load(), s.StreamFail.Load(), s.StreamRetry.Load(),
		s.DatagramSent.Load(), s.DatagramFail.Load(), s.DatagramRetry.Load(),
		s.MsgReceived.Load(),
		s.AvgRTTMs(), s.RTTMaxMs.Load(),
	)
}

// Snapshot 返回当前统计快照，便于日志或 JSON 输出
func (s *Stats) Snapshot() map[string]int64 {
	return map[string]int64{
		"stream_sent":    s.StreamSent.Load(),
		"stream_fail":    s.StreamFail.Load(),
		"stream_retry":   s.StreamRetry.Load(),
		"datagram_sent":  s.DatagramSent.Load(),
		"datagram_fail":  s.DatagramFail.Load(),
		"datagram_retry": s.DatagramRetry.Load(),
		"msg_received":   s.MsgReceived.Load(),
		"rtt_avg_ms":     int64(s.AvgRTTMs()),
		"rtt_max_ms":     s.RTTMaxMs.Load(),
		"rtt_sample_cnt": s.RTTCount.Load(),
	}
}

// WriteWithRetry 带重试的流写入，最多重试 maxRetry 次
func WriteWithRetry(stream *webtransport.Stream, data []byte, stats *Stats, maxRetry int) error {
	var err error
	for i := 0; i <= maxRetry; i++ {
		if i > 0 {
			stats.StreamRetry.Add(1)
			time.Sleep(time.Duration(i*50) * time.Millisecond)
		}
		_, err = stream.Write(data)
		if err == nil {
			stats.StreamSent.Add(1)
			return nil
		}
		log.Printf("[stream] 写入失败 (attempt %d/%d): %v", i+1, maxRetry+1, err)
	}
	stats.StreamFail.Add(1)
	return err
}

// SendDatagramWithRetry 带重试的数据报发送，最多重试 maxRetry 次
func SendDatagramWithRetry(session *webtransport.Session, data []byte, stats *Stats, maxRetry int) error {
	var err error
	for i := 0; i <= maxRetry; i++ {
		if i > 0 {
			stats.DatagramRetry.Add(1)
			time.Sleep(time.Duration(i*50) * time.Millisecond)
		}
		err = session.SendDatagram(data)
		if err == nil {
			stats.DatagramSent.Add(1)
			return nil
		}
		log.Printf("[datagram] 发送失败 (attempt %d/%d): %v", i+1, maxRetry+1, err)
	}
	stats.DatagramFail.Add(1)
	return err
}
