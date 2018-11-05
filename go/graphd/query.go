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

// The query portion of the graphd package contains request and response structures used in IO
// operations.

package graphd

import (
	"strings"
)

// A Request to be sent to a graphd database.
// Currently this is just a wrapper around a string.
type Request struct {
	body string
}

// String implements stringer interface for a Request.
func (r *Request) String() string {
	return strings.TrimSpace(r.body)
}

// A Response received from a graphd database.
// Currently this is just a wrapper around a string.
type Response struct {
	body string
}

// String implements stringer interface for a Response.
func (r *Response) String() string {
	return strings.TrimSpace(r.body)
}

// NewRequest returns a Request pointer initialized from the string parameter.  The parameter should
// represent one request to be sent to a graphd database.  A new line is automatically added.
func NewRequest(s string) *Request {
	var reqSB strings.Builder

	reqSB.WriteString(s)

	if strings.LastIndexByte(s, '\n') != len(s) {
		reqSB.WriteString("\n")
	}

	return &Request{reqSB.String()}
}

// joinRequests is used to fuse multiple Request pointers into a single, new-line delimited request.
// The joined Request pointer is returned.
func joinRequests(reqs ...*Request) *Request {
	var reqSB strings.Builder

	for _, req := range reqs {
		reqSB.WriteString(req.body)
	}

	return &Request{reqSB.String()}
}

// NewResponse returns a Response pointer initialized from the string parameter.  The parameter
// should represent one response received from a graphd database.
func NewResponse(s string) *Response {
	return &Response{s}
}
