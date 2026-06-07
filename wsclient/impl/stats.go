package impl

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// TotalSend 每次测试发送的总消息数
var TotalSend int64 = 2000

// Stats 与 WebTransport 客户端保持相同统计结构，便于横向对比
type Stats struct {
	Conn1Sent  atomic.Int64
	Conn1Fail  atomic.Int64
	Conn1Retry atomic.Int64
	Conn2Sent  atomic.Int64
	Conn2Fail  atomic.Int64
	Conn2Retry atomic.Int64

	MsgReceived atomic.Int64

	// RTT 统计（毫秒）
	RTTTotalMs atomic.Int64
	RTTCount   atomic.Int64
	RTTMaxMs   atomic.Int64

	// P95 分桶（桶宽 10ms）
	rttBuckets [1000]atomic.Int64

	// 收到的序号集合
	seqMu   sync.Mutex
	recvSeq map[int64]struct{}
}

func NewStats() *Stats {
	return &Stats{recvSeq: make(map[int64]struct{})}
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

// RecordReceived 收到一条回包，seq 为消息全局序号
func (s *Stats) RecordReceived(seq int64) {
	s.MsgReceived.Add(1)
	s.seqMu.Lock()
	s.recvSeq[seq] = struct{}{}
	s.seqMu.Unlock()
}

// ArrivalRate 返回到达率百分比
func (s *Stats) ArrivalRate(totalSent int64) float64 {
	if totalSent == 0 {
		return 0
	}
	return float64(s.MsgReceived.Load()) / float64(totalSent) * 100
}

// MissingSeqs 返回未收到的序号（仅前 20 个）
func (s *Stats) MissingSeqs(totalSent int64) []int64 {
	s.seqMu.Lock()
	defer s.seqMu.Unlock()
	var missing []int64
	for seq := int64(1); seq <= totalSent && len(missing) < 20; seq++ {
		if _, ok := s.recvSeq[seq]; !ok {
			missing = append(missing, seq)
		}
	}
	return missing
}

func (s *Stats) TotalSent() int64 {
	return s.Conn1Sent.Load() + s.Conn2Sent.Load()
}

func (s *Stats) String() string {
	sent := s.TotalSent()
	return fmt.Sprintf(
		"[Conn1] sent=%d fail=%d | [Conn2] sent=%d fail=%d | recv=%d | 到达率=%.1f%% | RTT avg=%.1fms p95=%dms max=%dms",
		s.Conn1Sent.Load(), s.Conn1Fail.Load(),
		s.Conn2Sent.Load(), s.Conn2Fail.Load(),
		s.MsgReceived.Load(),
		s.ArrivalRate(sent),
		s.AvgRTTMs(), s.P95RTTMs(), s.RTTMaxMs.Load(),
	)
}

func (s *Stats) Snapshot() map[string]any {
	sent := s.TotalSent()
	return map[string]any{
		"conn1_sent":     s.Conn1Sent.Load(),
		"conn1_fail":     s.Conn1Fail.Load(),
		"conn2_sent":     s.Conn2Sent.Load(),
		"conn2_fail":     s.Conn2Fail.Load(),
		"msg_received":   s.MsgReceived.Load(),
		"arrival_rate":   fmt.Sprintf("%.1f%%", s.ArrivalRate(sent)),
		"rtt_avg_ms":     int64(s.AvgRTTMs()),
		"rtt_p95_ms":     s.P95RTTMs(),
		"rtt_max_ms":     s.RTTMaxMs.Load(),
		"rtt_sample_cnt": s.RTTCount.Load(),
	}
}

// FinalReport 打印最终测试报告
func (s *Stats) FinalReport(totalSent int64) {
	missing := s.MissingSeqs(totalSent)
	log.Printf("=== 测试完成 ===")
	log.Printf("发送总数=%d 收到=%d 到达率=%.1f%%",
		totalSent, s.MsgReceived.Load(), s.ArrivalRate(totalSent))
	log.Printf("RTT avg=%.1fms p95=%dms max=%dms",
		s.AvgRTTMs(), s.P95RTTMs(), s.RTTMaxMs.Load())
	if len(missing) > 0 {
		log.Printf("未收到序号（前%d个）: %v", len(missing), missing)
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
