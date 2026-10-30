package workbench

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/vivaui/viva/flex"
	"github.com/vivaui/viva/ui"
	"motty/internal/sshconn"
)

const latencyProbeInterval = 3 * time.Second

// maybeProbeLatency starts one asynchronous ping when the latency display is
// enabled and the previous sample is old enough. The ping runs on its own
// goroutine and publishes through the session's atomics, so paint never waits
// for the network.
func (state *State) maybeProbeLatency(session *Session, canvas *ui.Canvas) {
	if !state.terminalShowLatency || session == nil || session.isLocal() || session.phase != sshconn.PhaseConnected {
		return
	}
	pinger, ok := session.conn.(pinger)
	if !ok {
		return
	}
	// A failed request cannot be canceled: its goroutine stays inside the SSH
	// client until the reply or the connection close. Stop probing this
	// connection instead of stacking another request every few seconds; the
	// next reconnect clears the flags.
	if atomic.LoadInt32(&session.rttBroken) == 1 {
		return
	}
	now := time.Now()
	if elapsed := now.Sub(session.rttLastPing); elapsed < latencyProbeInterval {
		if canvas != nil && atomic.LoadInt32(&session.rttAvailable) == 1 {
			canvas.RequestFrameIn(latencyProbeInterval - elapsed)
		}
		return
	}
	if !atomic.CompareAndSwapInt32(&session.rttMeasuring, 0, 1) {
		return
	}
	session.rttLastPing = now
	generation := atomic.LoadUint32(&session.rttGeneration)
	notify := state.notifyFor(session)
	go func() {
		defer func() {
			if atomic.LoadUint32(&session.rttGeneration) == generation {
				atomic.StoreInt32(&session.rttMeasuring, 0)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		rtt, err := pinger.Ping(ctx)
		if atomic.LoadUint32(&session.rttGeneration) != generation {
			return
		}
		if err != nil {
			atomic.StoreInt32(&session.rttBroken, 1)
			atomic.StoreInt32(&session.rttAvailable, 0)
			notify()
			return
		}
		ms := rtt.Milliseconds()
		if ms < 1 && rtt > 0 {
			ms = 1
		}
		atomic.StoreInt64(&session.rttMs, ms)
		atomic.StoreInt32(&session.rttAvailable, 1)
		notify()
	}()
}

// resetLatency discards the samples of the previous connection. A probe still
// in flight sees the new generation and drops its result.
func (session *Session) resetLatency() {
	atomic.AddUint32(&session.rttGeneration, 1)
	atomic.StoreInt32(&session.rttMeasuring, 0)
	atomic.StoreInt32(&session.rttBroken, 0)
	atomic.StoreInt32(&session.rttAvailable, 0)
	atomic.StoreInt64(&session.rttMs, 0)
	session.rttLastPing = time.Time{}
}

// latencyInfo returns the latency text and status color (<100ms green, <200ms yellow, >=200ms red).
func (state *State) latencyInfo(session *Session) (text string, color ui.Color, ok bool) {
	if !state.terminalShowLatency || session == nil || session.isLocal() || session.phase != sshconn.PhaseConnected {
		return "", ui.Color{}, false
	}
	if atomic.LoadInt32(&session.rttAvailable) != 1 {
		return "", ui.Color{}, false
	}
	ms := atomic.LoadInt64(&session.rttMs)
	if ms < 0 {
		return "", ui.Color{}, false
	}
	text = fmt.Sprintf("RTT %dms", ms)
	switch {
	case ms < 100:
		color = state.pal.success
	case ms < 200:
		color = state.pal.warning
	default:
		color = state.pal.danger
	}
	return text, color, true
}

// declareLatencyTag renders the latency badge in the bar.
func (state *State) declareLatencyTag(f *flex.Frame, canvas *ui.Canvas, session *Session) {
	latencyText, latencyColor, ok := state.latencyInfo(session)
	if !ok {
		return
	}
	textWidth := canvas.TextWidthWeight(latencyText, metrics.FontSizeS, ui.WeightMedium) + 2
	f.Row().PadSides(2, 6, 2, 6).MarginSides(0, 8, 0, 0).Items(flex.Center).
		Background(withAlpha(latencyColor, 0.14)).Radius(4).Children(func() {
		f.Text(latencyText).FontSize(metrics.FontSizeS).Weight(ui.WeightMedium).
			Color(latencyColor).LineHeight(16).Width(textWidth)
	})
}
