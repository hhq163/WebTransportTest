package impl

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
)

// TotalSend 每次测试发送的总消息数
var TotalSend int64 = 2000

// Message 统一的消息结构，所有类型共用
type Message struct {
	Type       string          `json:"type"`
	Seq        int64           `json:"seq"`
	Payload    json.RawMessage `json:"payload"`
	SendTimeMs int64           `json:"send_time_ms"`
}

// NewMessage 创建一条新消息
func NewMessage(msgType string, seq int64, payload json.RawMessage) *Message {
	return &Message{
		Type:       msgType,
		Seq:        seq,
		Payload:    payload,
		SendTimeMs: time.Now().UnixMilli(),
	}
}

// RTTMs 计算往返时延（毫秒）
func RTTMs(sendTimeMs int64) int64 {
	return time.Now().UnixMilli() - sendTimeMs
}

// Stats 全局统计，合并所有消息类型
type Stats struct {
	StartTime time.Time
	EndTime   time.Time

	Sent       atomic.Int64
	Received   atomic.Int64
	RTTTotalMs atomic.Int64
	RTTCount   atomic.Int64
	RTTMaxMs   atomic.Int64
	rttBuckets [1000]atomic.Int64 // 桶宽 10ms
}

func NewStats() *Stats {
	return &Stats{}
}

func (s *Stats) Start() {
	s.StartTime = time.Now()
}

func (s *Stats) Stop() {
	s.EndTime = time.Now()
}

func (s *Stats) RecordRTT(rttMs int64) {
	if rttMs < 0 {
		return
	}
	s.RTTTotalMs.Add(rttMs)
	s.RTTCount.Add(1)
	// 更新最大值
	for {
		cur := s.RTTMaxMs.Load()
		if rttMs <= cur || s.RTTMaxMs.CompareAndSwap(cur, rttMs) {
			break
		}
	}
	// 写入分桶
	idx := rttMs / 10
	if idx >= int64(len(s.rttBuckets)) {
		idx = int64(len(s.rttBuckets)) - 1
	}
	s.rttBuckets[idx].Add(1)
}

func (s *Stats) AvgRTTMs() float64 {
	cnt := s.RTTCount.Load()
	if cnt == 0 {
		return 0
	}
	return float64(s.RTTTotalMs.Load()) / float64(cnt)
}

func (s *Stats) percentileRTT(p int) int64 {
	total := s.RTTCount.Load()
	if total == 0 {
		return -1
	}
	threshold := total * int64(p) / 100
	var cumulative int64
	for i := range s.rttBuckets {
		cumulative += s.rttBuckets[i].Load()
		if cumulative >= threshold {
			return int64(i)*10 + 5
		}
	}
	return int64(len(s.rttBuckets)-1)*10 + 5
}

func (s *Stats) P95RTTMs() int64 {
	return s.percentileRTT(95)
}

func (s *Stats) P99RTTMs() int64 {
	return s.percentileRTT(99)
}

func (s *Stats) TotalSent() int64 {
	return s.Sent.Load()
}

func (s *Stats) TotalReceived() int64 {
	return s.Received.Load()
}

func (s *Stats) Duration() time.Duration {
	if s.EndTime.IsZero() {
		return time.Since(s.StartTime)
	}
	return s.EndTime.Sub(s.StartTime)
}

// Throughput 返回每秒消息数
func (s *Stats) Throughput() float64 {
	recv := s.TotalReceived()
	if recv == 0 {
		return 0
	}
	dur := s.Duration().Seconds()
	if dur <= 0 {
		return 0
	}
	return float64(recv) / dur
}

func (s *Stats) String() string {
	dur := s.Duration().Seconds()
	throughput := float64(s.TotalReceived()) / dur
	return fmt.Sprintf(
		"sent=%d recv=%d avg=%.1fms p95=%dms p99=%dms | throughput=%.1f/s",
		s.Sent.Load(), s.Received.Load(), s.AvgRTTMs(), s.P95RTTMs(), s.P99RTTMs(),
		throughput,
	)
}

func (s *Stats) FinalReport() {
	dur := s.Duration()
	log.Printf("=== 测试完成 ===")
	log.Printf("耗时: %v", dur)
	log.Printf("发送=%d 收到=%d avg=%.1fms p95=%dms p99=%dms max=%dms",
		s.Sent.Load(), s.Received.Load(),
		s.AvgRTTMs(), s.P95RTTMs(), s.P99RTTMs(), s.RTTMaxMs.Load())
	log.Printf("吞吐效率: %.1f 条/秒", s.Throughput())
}

// WriteWithRetry 带重试的流写入
func WriteWithRetry(stream *webtransport.Stream, data []byte, maxRetry int) error {
	var err error
	for i := 0; i <= maxRetry; i++ {
		_, err = stream.Write(data)
		if err == nil {
			return nil
		}
		if i < maxRetry {
			time.Sleep(time.Duration((i+1)*50) * time.Millisecond)
		}
	}
	return err
}
