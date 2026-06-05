package impl

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

const milestoneCount = 10000

// Stats 与 WebTransport 客户端保持相同统计结构，便于横向对比
// 所有时间在客户端本地计算，无跨机器时钟依赖
type Stats struct {
	// 连接 1（ping + chat）
	Conn1Sent  atomic.Int64
	Conn1Fail  atomic.Int64
	Conn1Retry atomic.Int64
	// 连接 2（game）
	Conn2Sent  atomic.Int64
	Conn2Fail  atomic.Int64
	Conn2Retry atomic.Int64

	MsgReceived atomic.Int64

	// RTT 统计（毫秒）
	RTTTotalMs atomic.Int64
	RTTCount   atomic.Int64
	RTTMaxMs   atomic.Int64

	// P95 分桶（桶宽 10ms，覆盖 0~9990ms）
	rttBuckets [1000]atomic.Int64

	// 里程碑：第 milestoneCount 条消息到达时间
	MilestoneTimeMs atomic.Int64
	StartTimeMs     atomic.Int64
}

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
	idx := rttMs / 10
	if idx >= int64(len(s.rttBuckets)) {
		idx = int64(len(s.rttBuckets)) - 1
	}
	s.rttBuckets[idx].Add(1)
}

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
			return int64(i)*10 + 5
		}
	}
	return int64(len(s.rttBuckets)-1)*10 + 5
}

func (s *Stats) AvgRTTMs() float64 {
	cnt := s.RTTCount.Load()
	if cnt == 0 {
		return -1
	}
	return float64(s.RTTTotalMs.Load()) / float64(cnt)
}

func (s *Stats) RecordReceived() {
	n := s.MsgReceived.Add(1)
	if n == 1 {
		s.StartTimeMs.CompareAndSwap(0, time.Now().UnixMilli())
	}
	if n == milestoneCount {
		ts := time.Now().UnixMilli()
		s.MilestoneTimeMs.Store(ts)
		elapsed := ts - s.StartTimeMs.Load()
		log.Printf("[milestone] 第 %d 条消息到达！耗时 %dms（%.1f 条/秒）",
			milestoneCount, elapsed, float64(milestoneCount)/float64(elapsed)*1000)
	}
}

func (s *Stats) MilestoneSummary() string {
	ts := s.MilestoneTimeMs.Load()
	recv := s.MsgReceived.Load()
	if ts == 0 {
		return fmt.Sprintf("第%d条未达到（当前 recv=%d）", milestoneCount, recv)
	}
	elapsed := ts - s.StartTimeMs.Load()
	return fmt.Sprintf("第%d条耗时=%dms 吞吐=%.1f条/秒",
		milestoneCount, elapsed, float64(milestoneCount)/float64(elapsed)*1000)
}

func (s *Stats) String() string {
	return fmt.Sprintf(
		"[Conn1] sent=%d fail=%d retry=%d | [Conn2] sent=%d fail=%d retry=%d | recv=%d | RTT avg=%.1fms p95=%dms max=%dms | %s",
		s.Conn1Sent.Load(), s.Conn1Fail.Load(), s.Conn1Retry.Load(),
		s.Conn2Sent.Load(), s.Conn2Fail.Load(), s.Conn2Retry.Load(),
		s.MsgReceived.Load(),
		s.AvgRTTMs(), s.P95RTTMs(), s.RTTMaxMs.Load(),
		s.MilestoneSummary(),
	)
}

func (s *Stats) Snapshot() map[string]any {
	return map[string]any{
		"conn1_sent":        s.Conn1Sent.Load(),
		"conn1_fail":        s.Conn1Fail.Load(),
		"conn1_retry":       s.Conn1Retry.Load(),
		"conn2_sent":        s.Conn2Sent.Load(),
		"conn2_fail":        s.Conn2Fail.Load(),
		"conn2_retry":       s.Conn2Retry.Load(),
		"msg_received":      s.MsgReceived.Load(),
		"rtt_avg_ms":        int64(s.AvgRTTMs()),
		"rtt_p95_ms":        s.P95RTTMs(),
		"rtt_max_ms":        s.RTTMaxMs.Load(),
		"rtt_sample_cnt":    s.RTTCount.Load(),
		"milestone_time_ms": s.MilestoneTimeMs.Load(),
		"start_time_ms":     s.StartTimeMs.Load(),
	}
}

// WriteWithRetry 带重试的 WebSocket 写入
func WriteWithRetry(writeFn func([]byte) error, data []byte, sent, fail, retry *atomic.Int64, maxRetry int) error {
	var err error
	for i := 0; i <= maxRetry; i++ {
		if i > 0 {
			retry.Add(1)
			time.Sleep(time.Duration(i*50) * time.Millisecond)
		}
		err = writeFn(data)
		if err == nil {
			sent.Add(1)
			return nil
		}
		log.Printf("[ws] 写入失败 (attempt %d/%d): %v", i+1, maxRetry+1, err)
	}
	fail.Add(1)
	return err
}
