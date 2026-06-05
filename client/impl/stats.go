package impl

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
)

// milestoneCount 第 N 条消息到达时记录时间戳
const milestoneCount = 10000

// Stats 记录客户端消息收发及 RTT 统计，所有字段均线程安全
// 所有时间均在客户端计算，避免与服务端时钟不一致导致误差
type Stats struct {
	StreamSent    atomic.Int64
	StreamFail    atomic.Int64
	StreamRetry   atomic.Int64
	DatagramSent  atomic.Int64
	DatagramFail  atomic.Int64
	DatagramRetry atomic.Int64
	MsgReceived   atomic.Int64

	// RTT 统计（毫秒，客户端发送时打时间戳，收到回包时计算，无需服务端参与）
	RTTTotalMs atomic.Int64
	RTTCount   atomic.Int64
	RTTMaxMs   atomic.Int64

	// P95 RTT：简单分桶计数器，桶宽 10ms，覆盖 0~9990ms
	rttBuckets [1000]atomic.Int64

	// 第 milestoneCount 条消息到达的时间戳（Unix 毫秒，0 表示未达到）
	MilestoneTimeMs atomic.Int64
	// 测试开始时间（首条消息发出时记录）
	StartTimeMs atomic.Int64
}

// RecordRTT 记录一次往返延迟（毫秒），所有计算在客户端完成
func (s *Stats) RecordRTT(rttMs int64) {
	if rttMs < 0 {
		return
	}
	s.RTTTotalMs.Add(rttMs)
	s.RTTCount.Add(1)

	// 更新最大值（CAS）
	for {
		cur := s.RTTMaxMs.Load()
		if rttMs <= cur {
			break
		}
		if s.RTTMaxMs.CompareAndSwap(cur, rttMs) {
			break
		}
	}

	// 写入分桶（桶宽 10ms）
	idx := rttMs / 10
	if idx >= int64(len(s.rttBuckets)) {
		idx = int64(len(s.rttBuckets)) - 1
	}
	s.rttBuckets[idx].Add(1)
}

// P95RTTMs 返回 P95 RTT（毫秒），无样本时返回 -1
func (s *Stats) P95RTTMs() int64 {
	total := s.RTTCount.Load()
	if total == 0 {
		return -1
	}
	threshold := total*95/100 + 1
	var cumulative int64
	for i, bucket := range s.rttBuckets {
		cumulative += bucket.Load()
		if cumulative >= threshold {
			return int64(i)*10 + 5 // 桶中点
		}
	}
	return int64(len(s.rttBuckets)-1)*10 + 5
}

// AvgRTTMs 返回平均 RTT（毫秒），无样本时返回 -1
func (s *Stats) AvgRTTMs() float64 {
	cnt := s.RTTCount.Load()
	if cnt == 0 {
		return -1
	}
	return float64(s.RTTTotalMs.Load()) / float64(cnt)
}

// RecordReceived 每次客户端收到一条回包时调用，维护到达计数和里程碑时间
func (s *Stats) RecordReceived() {
	n := s.MsgReceived.Add(1)

	// 记录测试开始时间（第 1 条）
	if n == 1 {
		s.StartTimeMs.CompareAndSwap(0, time.Now().UnixMilli())
	}

	// 记录第 milestoneCount 条消息到达时间
	if n == milestoneCount {
		ts := time.Now().UnixMilli()
		s.MilestoneTimeMs.Store(ts)
		elapsed := ts - s.StartTimeMs.Load()
		log.Printf("[milestone] 第 %d 条消息到达！耗时 %dms（%.1f 条/秒）",
			milestoneCount, elapsed, float64(milestoneCount)/float64(elapsed)*1000)
	}
}

// MilestoneSummary 返回里程碑统计文字，未达到时返回进度
func (s *Stats) MilestoneSummary() string {
	ts := s.MilestoneTimeMs.Load()
	recv := s.MsgReceived.Load()
	if ts == 0 {
		return fmt.Sprintf("第%d条未达到（当前 recv=%d）", milestoneCount, recv)
	}
	elapsed := ts - s.StartTimeMs.Load()
	throughput := float64(milestoneCount) / float64(elapsed) * 1000
	return fmt.Sprintf("第%d条耗时=%dms 吞吐=%.1f条/秒", milestoneCount, elapsed, throughput)
}

func (s *Stats) String() string {
	return fmt.Sprintf(
		"[Stream] sent=%d fail=%d retry=%d | [Datagram] sent=%d fail=%d retry=%d | recv=%d | RTT avg=%.1fms p95=%dms max=%dms | %s",
		s.StreamSent.Load(), s.StreamFail.Load(), s.StreamRetry.Load(),
		s.DatagramSent.Load(), s.DatagramFail.Load(), s.DatagramRetry.Load(),
		s.MsgReceived.Load(),
		s.AvgRTTMs(), s.P95RTTMs(), s.RTTMaxMs.Load(),
		s.MilestoneSummary(),
	)
}

// Snapshot 返回当前统计快照，便于 JSON 输出
func (s *Stats) Snapshot() map[string]int64 {
	return map[string]int64{
		"stream_sent":       s.StreamSent.Load(),
		"stream_fail":       s.StreamFail.Load(),
		"stream_retry":      s.StreamRetry.Load(),
		"datagram_sent":     s.DatagramSent.Load(),
		"datagram_fail":     s.DatagramFail.Load(),
		"datagram_retry":    s.DatagramRetry.Load(),
		"msg_received":      s.MsgReceived.Load(),
		"rtt_avg_ms":        int64(s.AvgRTTMs()),
		"rtt_p95_ms":        s.P95RTTMs(),
		"rtt_max_ms":        s.RTTMaxMs.Load(),
		"rtt_sample_cnt":    s.RTTCount.Load(),
		"milestone_time_ms": s.MilestoneTimeMs.Load(),
		"start_time_ms":     s.StartTimeMs.Load(),
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
