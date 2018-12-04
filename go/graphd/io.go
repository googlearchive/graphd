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

// The io portion of the graphd package facilitates IO operations against a graphd database.

package graphd

import (
	"bufio"
	"errors"
	"fmt"
)

// readReponses returns a Response pointer slice read from an established connection to a graphd
// database and a nil error on success.  On any failure to read a response, a Response pointer
// slice containing zero-value Response pointers for the given failures, and the last encountered
// error are returned.  readResponses must be called with g.conn locked.
// TODO: Would it be useful to return all encountered errors?  Any chance after a failure that
//       we'll block on the following call to ReadString()?
func (g *graphd) readResponses(reqsNum int) ([]*Response, error) {
	var retErr error
	var resSlice []*Response

	reader := bufio.NewReader(g.conn.netConn)

	for i := 0; i < reqsNum; i++ {
		str, err := reader.ReadString('\n')
		if err != nil {
			resSlice = append(resSlice, NewResponse(""))
			retErr = err
		} else {
			resSlice = append(resSlice, NewResponse(str))
		}
	}

	return resSlice, retErr
}

// Query attempts to send the Request to the graphd database to which this instance of the library
// is connected.  In the case of more than one Request, the requests are joined and sent as one.
// If no connection is currently present, or if the established connection is stale, Query will
// trigger a Redial.  A Response pointer slice containing responses from the graphd database (failed
// responses are zero-value value Response pointers) is returned along with an error code.  In the
// case of any failed responses, the returned error code will contain the last encountered error.
// Query locks the connection, allowing one thread to Query at a time.
func (g *graphd) Query(reqs ...*Request) ([]*Response, error) {
	g.conn.Lock()

	// Join requests into one if needed.
	var req *Request
	reqsNum := len(reqs)
	if reqsNum > 1 {
		req = joinRequests(reqs...)
	} else {
		req = reqs[0]
	}

	g.LogDebugf("attempting to send '%v'", req)

	sent := false
	retries := 2
	for sent == false {
		var err error
		var errStr string

		switch g.conn.exists() {
		// An established connection is present, try to send.
		case true:
			// Queries to graphd are new line terminated.
			_, err = fmt.Fprintf(g.conn.netConn, "%v", req.body)
			if err != nil {
				// Set base error for failed send.
				errStr = fmt.Sprintf("failed to send '%v': %v", req, err)
				retries--
				// If we've exhausted our retries, log and return error.
				if retries == 0 {
					g.LogErr(errStr)
					g.conn.Unlock()
					return []*Response{NewResponse("")}, errors.New(errStr)
				}
				// We can still retry, so try a Redial.  If it fails, append the error message to
				// the base error, log and return the error.
				g.conn.Unlock()
				if err = g.Redial(0); err != nil {
					errStr = fmt.Sprintf("%v: %v", errStr, err)
					g.LogErr(errStr)
					return []*Response{NewResponse("")}, errors.New(errStr)
				}
				// OK, we've redialed.  Lock the connection and let's try that send again.
				g.conn.Lock()
				g.LogErrf("%v: retrying (%v retries left)", errStr, retries)
			} else {
				// We've successfully sent.
				g.LogDebugf("successfully sent '%v'", req)
				sent = true
			}

		// No connection present, try to Dial.
		case false:
			g.conn.Unlock()
			err = g.Dial(0)
			if err != nil {
				errStr = fmt.Sprintf("failed to send '%v': %v", req, err)
				g.LogErr(errStr)
				return []*Response{NewResponse("")}, errors.New(errStr)
			}
			g.conn.Lock()
		}
	}

	// We've successfully sent a query, now grab the responses and return them.
	res, err := g.readResponses(reqsNum)
	if err != nil {
		errStr := fmt.Sprintf("failed to receive response to '%v': %v", req, err)
		g.LogErr(errStr)
		err = errors.New(errStr)
	} else {
		g.LogDebugf("received response '%v'", res)
	}
	g.conn.Unlock()
	return res, err
}
