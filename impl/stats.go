package impl

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
)

// Stats 记录消息收发及延迟统计，所有字段均使用原子操作，线程安全
type Stats struct {
	StreamSent    atomic.Int64
	StreamFail    atomic.Int64
	StreamRetry   atomic.Int64
	DatagramSent  atomic.Int64
	DatagramFail  atomic.Int64
	DatagramRetry atomic.Int64
	MsgReceived   atomic.Int64

	// 延迟统计（单向延迟，毫秒）
	DelayTotalMs atomic.Int64 // 所有已收消息延迟之和
	DelayCount   atomic.Int64 // 有效延迟样本数
	DelayMaxMs   atomic.Int64 // 历史最大延迟
}

// RecordDelay 记录一次消息延迟（毫秒），delayMs < 0 表示无效跳过
func (s *Stats) RecordDelay(delayMs int64) {
	if delayMs < 0 {
		return
	}
	s.DelayTotalMs.Add(delayMs)
	s.DelayCount.Add(1)
	// CAS 更新最大值
	for {
		cur := s.DelayMaxMs.Load()
		if delayMs <= cur {
			break
		}
		if s.DelayMaxMs.CompareAndSwap(cur, delayMs) {
			break
		}
	}
}

// AvgDelayMs 返回平均延迟（毫秒），无样本时返回 -1
func (s *Stats) AvgDelayMs() float64 {
	cnt := s.DelayCount.Load()
	if cnt == 0 {
		return -1
	}
	return float64(s.DelayTotalMs.Load()) / float64(cnt)
}

func (s *Stats) String() string {
	return fmt.Sprintf(
		"[Stream] sent=%d fail=%d retry=%d | [Datagram] sent=%d fail=%d retry=%d | recv=%d | delay avg=%.1fms max=%dms",
		s.StreamSent.Load(), s.StreamFail.Load(), s.StreamRetry.Load(),
		s.DatagramSent.Load(), s.DatagramFail.Load(), s.DatagramRetry.Load(),
		s.MsgReceived.Load(),
		s.AvgDelayMs(), s.DelayMaxMs.Load(),
	)
}

// Snapshot 返回当前统计的不可变快照，便于 JSON 序列化
func (s *Stats) Snapshot() map[string]int64 {
	return map[string]int64{
		"stream_sent msg cnt=":    s.StreamSent.Load(),
		"stream_fail msg cnt=":    s.StreamFail.Load(),
		"stream_retry msg cnt=":   s.StreamRetry.Load(),
		"datagram_sent msg cnt=":  s.DatagramSent.Load(),
		"datagram_fail msg cnt=":  s.DatagramFail.Load(),
		"datagram_retry msg cnt=": s.DatagramRetry.Load(),
		"msg_received msg cnt=":   s.MsgReceived.Load(),
		"delay_avg_ms":            int64(s.AvgDelayMs()),
		"delay_max_ms":            s.DelayMaxMs.Load(),
		"delay_sample_cnt":        s.DelayCount.Load(),
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
