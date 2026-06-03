package impl

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	webtransport "github.com/quic-go/webtransport-go"
)

// SessionInfo 保存单个会话的元数据
type SessionInfo struct {
	ID          string
	Session     *webtransport.Session
	ConnectedAt time.Time
	RemoteAddr  string
}

// SessionManager 管理所有活跃 WebTransport 会话，线程安全
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*SessionInfo
	counter  atomic.Int64 // 用于生成自增 ID
}

func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]*SessionInfo),
	}
}

// Add 注册一个新会话，返回分配的 ID
func (m *SessionManager) Add(session *webtransport.Session, remoteAddr string) string {
	id := fmt.Sprintf("sess-%d", m.counter.Add(1))
	m.mu.Lock()
	m.sessions[id] = &SessionInfo{
		ID:          id,
		Session:     session,
		ConnectedAt: time.Now(),
		RemoteAddr:  remoteAddr,
	}
	m.mu.Unlock()
	return id
}

// Remove 注销会话
func (m *SessionManager) Remove(id string) {
	m.mu.Lock()
	delete(m.sessions, id)
	m.mu.Unlock()
}

// Get 按 ID 查找会话
func (m *SessionManager) Get(id string) (*SessionInfo, bool) {
	m.mu.RLock()
	info, ok := m.sessions[id]
	m.mu.RUnlock()
	return info, ok
}

// Count 返回当前活跃会话数
func (m *SessionManager) Count() int {
	m.mu.RLock()
	n := len(m.sessions)
	m.mu.RUnlock()
	return n
}

// Snapshot 返回所有会话的快照，用于 HTTP 接口展示
func (m *SessionManager) Snapshot() []map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]map[string]any, 0, len(m.sessions))
	for _, info := range m.sessions {
		result = append(result, map[string]any{
			"id":           info.ID,
			"remote_addr":  info.RemoteAddr,
			"connected_at": info.ConnectedAt.Format(time.RFC3339),
			"duration_s":   int(time.Since(info.ConnectedAt).Seconds()),
		})
	}
	return result
}

// Broadcast 向所有活跃会话发送数据报
func (m *SessionManager) Broadcast(data []byte) {
	m.mu.RLock()
	sessions := make([]*webtransport.Session, 0, len(m.sessions))
	for _, info := range m.sessions {
		sessions = append(sessions, info.Session)
	}
	m.mu.RUnlock()

	for _, sess := range sessions {
		_ = sess.SendDatagram(data)
	}
}
