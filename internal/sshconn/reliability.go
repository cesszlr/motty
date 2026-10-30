package sshconn

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"golang.org/x/crypto/ssh"
)

const keepAliveRequest = "keepalive@openssh.com"

type keepAliveWorker struct {
	stop chan struct{}
	done chan struct{}
}

func (config Config) validate() error {
	switch {
	case config.DialTimeout < 0:
		return errors.New("sshconn: dial timeout must not be negative")
	case config.KeepAlive.Interval < 0:
		return errors.New("sshconn: keepalive interval must not be negative")
	case config.KeepAlive.Timeout < 0:
		return errors.New("sshconn: keepalive timeout must not be negative")
	case config.KeepAlive.Interval == 0 && config.KeepAlive.Timeout != 0:
		return errors.New("sshconn: keepalive timeout requires an interval")
	case config.KeepAlive.Interval > 0 && config.KeepAlive.Timeout == 0:
		return errors.New("sshconn: keepalive timeout must be positive when enabled")
	default:
		return nil
	}
}

func (conn *Conn) dialThrough(client *ssh.Client, address string) (net.Conn, error) {
	ctx := conn.ctx
	cancel := func() {}
	if conn.config.DialTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, conn.config.DialTimeout)
	}
	defer cancel()
	return client.DialContext(ctx, "tcp", address)
}

func (conn *Conn) startKeepAlive(client *ssh.Client) *keepAliveWorker {
	if conn.config.KeepAlive.Interval == 0 {
		return nil
	}
	worker := &keepAliveWorker{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(worker.done)
		conn.keepAlive(client, worker.stop)
	}()
	return worker
}

func (conn *Conn) keepAlive(client *ssh.Client, stop <-chan struct{}) {
	ticker := time.NewTicker(conn.config.KeepAlive.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-conn.ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			select {
			case <-conn.ctx.Done():
				return
			case <-stop:
				return
			default:
			}
			if !conn.sendKeepAlive(client) {
				return
			}
		}
	}
}

func (conn *Conn) sendKeepAlive(client *ssh.Client) bool {
	timedOut := make(chan struct{})
	timer := time.AfterFunc(conn.config.KeepAlive.Timeout, func() {
		if conn.recordDisconnect(fmt.Errorf("%w: %w", ErrDisconnected, ErrKeepAliveTimeout)) {
			_ = client.Close()
		}
		close(timedOut)
	})
	_, _, err := client.SendRequest(keepAliveRequest, true, nil)
	if !timer.Stop() {
		<-timedOut
		return false
	}
	if err == nil {
		return true
	}
	if conn.ctx.Err() == nil && conn.recordDisconnect(fmt.Errorf("%w: keepalive: %w", ErrDisconnected, err)) {
		_ = client.Close()
	}
	return false
}

func (conn *Conn) recordDisconnect(err error) bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.ctx.Err() != nil || conn.disconnectErr != nil {
		return false
	}
	conn.disconnectErr = err
	return true
}

func (conn *Conn) closedError(waitErr error) error {
	if conn.ctx.Err() != nil {
		return nil
	}
	conn.mu.Lock()
	err := conn.disconnectErr
	conn.mu.Unlock()
	if err != nil {
		return err
	}
	if waitErr == nil {
		return ErrDisconnected
	}
	return fmt.Errorf("%w: %w", ErrDisconnected, waitErr)
}

// Ping measures the round-trip time (RTT) to the SSH server using
// keepalive@openssh.com.
//
// A context without a deadline gets the default two-second one. The request
// itself cannot be canceled: after a timeout the client keeps waiting for the
// reply before it can send another global request, so callers must not start a
// new probe while the previous one is unresolved.
func (conn *Conn) Ping(ctx context.Context) (time.Duration, error) {
	conn.mu.Lock()
	client := conn.client
	phase := conn.status.Phase
	conn.mu.Unlock()
	if client == nil || phase != PhaseConnected {
		return 0, errors.New("sshconn: not connected")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	}
	type pingResult struct {
		rtt time.Duration
		err error
	}
	ch := make(chan pingResult, 1)
	start := time.Now()
	go func() {
		_, _, err := client.SendRequest(keepAliveRequest, true, nil)
		ch <- pingResult{rtt: time.Since(start), err: err}
	}()
	select {
	case res := <-ch:
		return res.rtt, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}
