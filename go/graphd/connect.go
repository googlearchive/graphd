// Copyright 2018 Google Inc. All rights reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The connect portion of the graphd package facilitates connectivty to a graphd database.

package graphd

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"
)

// A lockable net.Conn.
type connection struct {
	sync.Mutex
	netConn net.Conn
}

// primeConnection prepares and returns a connection structure pointer.
func primeConnection() *connection {
	conn := connection{}
	return &conn
}

// exists only checks if this graphd instance has a non-nil connection (net.Conn) with a graph
// database.  The connection itself may be stale, but any failed writes will trigger a Redial.
// Must hold connection lock to invoke.
func (c *connection) exists() bool {
	return c.netConn != nil
}

// Connections are sent/received on res.  awaitingConn is used as a mutex.
type connChan struct {
	res          chan net.Conn
	awaitingConn chan int
}

// Returns a ready to use connCh struct.
func initConnCh() connChan {
	connCh := connChan{}
	connCh.res = make(chan net.Conn)
	connCh.awaitingConn = make(chan int)
	return connCh
}

// dial attempts to acquire a connection to url with the specified timeout.  On connection
// success or failure, dial will attempt to send the net.Conn over the result channel if it
// can read from the awaitingConn channel (used as mutex here).  If the read fails (awaitingConn
// has been closed by the caller), dial will attempt to close the acquired connection.
func (g *graphd) dial(url *url.URL, timeout time.Duration, connCh connChan) {
	// A timeout of zero value is equivalent to no timeout.
	dialer := net.Dialer{Timeout: timeout}

	// Attempt to dial.  Log error, but send conn anyway.  A nil conn will be discarded by the
	// caller.
	g.LogDebugf("dialing %v with timeout %v", url, timeout)
	conn, err := dialer.Dial(url.Scheme, url.Host)
	if err != nil {
		g.LogErrf("failed to dial %v: %v", url, err)
	}

	// If an acquired connection has not yet been sent by a dilaer, go ahead and send the conn,
	// regardless if it's nil, and return.
	if _, ok := <-connCh.awaitingConn; ok {
		if conn != nil {
			g.LogDebugf("sending acquired connection to %v on channel", url)
		} else {
			g.LogDebugf("sending nil connection to %v on channel", url)
		}
		connCh.res <- conn
		return
	}

	// A connection was already acquired by another dialer.  If we did acquire a connection,
	// close it.
	if conn != nil {
		g.LogDebugf("discarding and closing acquired connection to %v", url)
		if err := conn.Close(); err != nil {
			g.LogErrf("failed to close acquired connection to %v, resource leak", url)
		}
	}
}

// Dial a graphd database.  Dial will attempt to connect to all URLs found in the URLs list associated
// with this graphd instance, retaining the first successful connection.  On failure, an appropriate
// error code is returned.  If an acquired connection is already present and valid, Dial returns nil.
// Timeout is specified in seconds.  A timeout of 0 is treated as no timeout.
// Dial ensures only one thread is dialing at a time.
func (g *graphd) Dial(t int) error {
	// Set timeout if t > 0, otherwise use the zero value (0s) which signals no timeout.
	var timeout time.Duration
	if t > 0 {
		timeout = time.Duration(t) * time.Second
	}

	// If URLs list is empty, return error.
	if len(g.urls) == 0 {
		errStr := fmt.Sprintf("no URL found in %v", g.urls)
		g.LogErrf(errStr)
		return errors.New(errStr)
	}

	// Only one thread dialing at a time.
	g.conn.Lock()
	defer g.conn.Unlock()

	// If already connected, return success.
	if g.conn.exists() {
		g.LogDebugf("already connected to %v", g.conn.netConn.RemoteAddr())
		return nil
	}

	// In parallel, for each URL in the list, send a connection request along with the timeout.
	g.LogDebugf("attempting to connect to %v", g.urls)
	connCh := initConnCh()
	numDialers := 0
	for _, url := range g.urls {
		go g.dial(url, timeout, connCh)
		numDialers++
	}

	// Loop and wait for connections from dialers.  Send an int to signal we're waiting for a connection.
	// Once we've acquired a valid connection, close our signal channel, preventing other dialers from
	// sending further connections, and return success.
	for numDialers > 0 {
		connCh.awaitingConn <- 1
		conn := <-connCh.res
		numDialers--

		// If the connection is invalid, continue listening.
		if conn == nil {
			continue
		}

		// Acquired a valid connection.  Set connection instance.
		g.LogDebugf("successfully connected to %v", conn.RemoteAddr())
		g.conn.netConn = conn

		// Signal dialers that we've acquired a connection.
		close(connCh.awaitingConn)

		// Return success.
		return nil
	}

	// If no valid connection is acquired, return error
	errStr := fmt.Sprintf("failed to connect to any URL in %v", g.urls)
	g.LogErr(errStr)
	return errors.New(errStr)
}

// Disconnect attempts to close the existing connection to a graphd database.  On success, nil is
// returned.  On failure, an error is returned.  Regardless if the connection was properly closed,
// the connection is zeroed out.
func (g *graphd) Disconnect() error {
	// Only one thread at a time allowed to Disconnect.
	g.conn.Lock()
	defer g.conn.Unlock()

	// If not connected, return success.
	if !g.conn.exists() {
		g.LogDebug("no connection present")
		return nil
	}

	// Zero out the connection on function exit.
	defer func() { g.conn.netConn = nil }()

	// Retain address for logs.
	connectedToAddr := g.conn.netConn.RemoteAddr()

	// Try to close the existing connection.
	err := g.conn.netConn.Close()
	if err != nil {
		errStr := fmt.Sprintf("failed to close existing connection, resource leak: %v", err)
		g.LogErr(errStr)
		return errors.New(errStr)
	}

	// Return success.
	g.LogDebugf("successfully disconnected from %v", connectedToAddr)
	return nil
}

// Redial first disconnects the existing connection, and calls Dial with the user provided
// timeout (in seconds).  Redial returns the error code returned by Dial.
func (g *graphd) Redial(t int) error {
	// Try to disconnect.  Continue with redial despite any failure.
	g.Disconnect()

	// Dial.
	if err := g.Dial(t); err != nil {
		errStr := fmt.Sprintf("failed to reconnect: %v", err)
		g.LogErr(errStr)
		return errors.New(errStr)
	}

	// Return success.
	return nil
}
