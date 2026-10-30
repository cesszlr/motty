package workbench

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"motty/internal/sshconn"
)

type mockPingerConn struct {
	fakeConn
	rtt time.Duration
	err error
}

func (m *mockPingerConn) Ping(ctx context.Context) (time.Duration, error) {
	return m.rtt, m.err
}

func TestLatencyToggleAndPersistence(t *testing.T) {
	options := testOptions(t)
	state := newTestState(t, options)

	// 默认状态下显示延迟应为关闭
	if state.terminalShowLatency {
		t.Fatal("expected terminalShowLatency to be false by default")
	}

	// 点击设置中的切换开关
	state.Click(hitSettingsLatencyToggle)
	if !state.terminalShowLatency {
		t.Fatal("expected terminalShowLatency to be true after click")
	}

	// 模拟重启恢复状态
	restarted := newTestState(t, options)
	if !restarted.terminalShowLatency {
		t.Fatal("expected terminalShowLatency to survive restart")
	}

	// 再次点击关闭
	restarted.Click(hitSettingsLatencyToggle)
	if restarted.terminalShowLatency {
		t.Fatal("expected terminalShowLatency to be false after second click")
	}

	// 再次重启确认关闭状态已保存
	restartedAgain := newTestState(t, options)
	if restartedAgain.terminalShowLatency {
		t.Fatal("expected terminalShowLatency false to survive restart")
	}
}

func TestLatencyColorThresholds(t *testing.T) {
	state := newTestState(t, testOptions(t))
	state.terminalShowLatency = true

	session := &Session{
		phase: sshconn.PhaseConnected,
	}

	// 1. 小于 100ms：绿色 (state.pal.success)
	atomic.StoreInt64(&session.rttMs, 50)
	atomic.StoreInt32(&session.rttAvailable, 1)
	text, color, ok := state.latencyInfo(session)
	if !ok || text != "RTT 50ms" || color != state.pal.success {
		t.Fatalf("expected RTT 50ms green (%v), got text=%q, color=%v, ok=%v", state.pal.success, text, color, ok)
	}

	atomic.StoreInt64(&session.rttMs, 99)
	text, color, ok = state.latencyInfo(session)
	if !ok || text != "RTT 99ms" || color != state.pal.success {
		t.Fatalf("expected RTT 99ms green, got text=%q, color=%v, ok=%v", text, color, ok)
	}

	// 2. 小于 200ms（>= 100ms）：黄色 (state.pal.warning)
	atomic.StoreInt64(&session.rttMs, 100)
	text, color, ok = state.latencyInfo(session)
	if !ok || text != "RTT 100ms" || color != state.pal.warning {
		t.Fatalf("expected RTT 100ms yellow (%v), got text=%q, color=%v, ok=%v", state.pal.warning, text, color, ok)
	}

	atomic.StoreInt64(&session.rttMs, 199)
	text, color, ok = state.latencyInfo(session)
	if !ok || text != "RTT 199ms" || color != state.pal.warning {
		t.Fatalf("expected RTT 199ms yellow, got text=%q, color=%v, ok=%v", text, color, ok)
	}

	// 3. 大于等于 200ms：红色 (state.pal.danger)
	atomic.StoreInt64(&session.rttMs, 200)
	text, color, ok = state.latencyInfo(session)
	if !ok || text != "RTT 200ms" || color != state.pal.danger {
		t.Fatalf("expected RTT 200ms red (%v), got text=%q, color=%v, ok=%v", state.pal.danger, text, color, ok)
	}

	atomic.StoreInt64(&session.rttMs, 350)
	text, color, ok = state.latencyInfo(session)
	if !ok || text != "RTT 350ms" || color != state.pal.danger {
		t.Fatalf("expected RTT 350ms red, got text=%q, color=%v, ok=%v", text, color, ok)
	}
}

func TestLatencyHiddenWhenDisabledOrLocal(t *testing.T) {
	state := newTestState(t, testOptions(t))
	state.terminalShowLatency = false

	session := &Session{
		phase: sshconn.PhaseConnected,
	}
	atomic.StoreInt64(&session.rttMs, 40)
	atomic.StoreInt32(&session.rttAvailable, 1)

	// 开关关闭时应该不显示
	if _, _, ok := state.latencyInfo(session); ok {
		t.Fatal("expected latency to be hidden when terminalShowLatency is false")
	}

	// 开启开关
	state.terminalShowLatency = true
	if _, _, ok := state.latencyInfo(session); !ok {
		t.Fatal("expected latency to be shown when enabled")
	}

	// 本地 shell 应该不显示
	session.HostID = localHostID
	if _, _, ok := state.latencyInfo(session); ok {
		t.Fatal("expected latency to be hidden for local sessions")
	}

	// 未就绪时不显示
	session.HostID = 100
	atomic.StoreInt32(&session.rttAvailable, 0)
	if _, _, ok := state.latencyInfo(session); ok {
		t.Fatal("expected latency to be hidden when not available")
	}
}

func TestMaybeProbeLatency(t *testing.T) {
	state := newTestState(t, testOptions(t))
	state.terminalShowLatency = true

	pingerConn := &mockPingerConn{rtt: 42 * time.Millisecond}
	session := &Session{
		ID:    1,
		conn:  pingerConn,
		phase: sshconn.PhaseConnected,
	}

	state.maybeProbeLatency(session, nil)

	// 等待异步 goroutine 完成
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&session.rttAvailable) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if atomic.LoadInt32(&session.rttAvailable) != 1 {
		t.Fatal("expected rttAvailable to be set to 1")
	}
	if got := atomic.LoadInt64(&session.rttMs); got != 42 {
		t.Fatalf("expected rttMs to be 42, got %d", got)
	}
}
