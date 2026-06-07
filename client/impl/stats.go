package impl

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
)

// TotalSend 每次测试发送的总消息数，main 中初始化后设置
var TotalSend int64 = 2000

// Stats 记录客户端消息收发及 RTT 统计，所有字段均线程安全
// 所有时间均在客户端计算，避免与服务端时钟不一致导致误差
type Stats struct {
	// OnAllSent 所有消息发送完毕后由 main 调用，触发等待+退出
	OnAllSent func()

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

	// P95 RTT 分桶（桶宽 10ms）
	rttBuckets [1000]atomic.Int64

	// 收到的序号集合（用于计算到达率）
	seqMu   sync.Mutex
	recvSeq map[int64]struct{}
}

func NewStats() *Stats {
	return &Stats{recvSeq: make(map[int64]struct{})}
}

// RecordRTT 记录一次 RTT（毫秒）
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

// P95RTTMs 返回 P95 RTT（毫秒）
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

// RecordReceived 收到一条回包，seq 为消息序号
func (s *Stats) RecordReceived(seq int64) {
	s.MsgReceived.Add(1)
	s.seqMu.Lock()
	s.recvSeq[seq] = struct{}{}
	s.seqMu.Unlock()
}

// ArrivalRate 返回到达率（已收 / 已发），只在发送完成后调用
func (s *Stats) ArrivalRate(totalSent int64) float64 {
	if totalSent == 0 {
		return 0
	}
	return float64(s.MsgReceived.Load()) / float64(totalSent) * 100
}

// MissingSeqs 返回未收到回包的序号列表（仅前 20 个）
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

func (s *Stats) String() string {
	sent := s.StreamSent.Load()
	return fmt.Sprintf(
		"[Stream] sent=%d fail=%d retry=%d | recv=%d | 到达率=%.1f%% | RTT avg=%.1fms p95=%dms max=%dms",
		sent, s.StreamFail.Load(), s.StreamRetry.Load(),
		s.MsgReceived.Load(),
		s.ArrivalRate(sent),
		s.AvgRTTMs(), s.P95RTTMs(), s.RTTMaxMs.Load(),
	)
}

func (s *Stats) Snapshot() map[string]any {
	sent := s.StreamSent.Load()
	return map[string]any{
		"stream_sent":    sent,
		"stream_fail":    s.StreamFail.Load(),
		"stream_retry":   s.StreamRetry.Load(),
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

// WriteWithRetry 带重试的流写入
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

// SendDatagramWithRetry 带重试的数据报发送
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
