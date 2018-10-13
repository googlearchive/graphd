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

// Package graphd implements a library for communicating with a graphd database.

package graphd

import (
	"log/syslog"
)

// The interface to a running graphdb instance.
type graphd struct {
	logger *graphdLogger
}

// New returns a populated graphdb struct pointer.
// l can be used to specify one's own logger (must implement Print).  A nil Logger interface
// argument will default to using syslog.
// logLevel is used to control which log messages are emitted.
func New(l Logger, logLevel syslog.Priority) *graphd {
	g := &graphd{}

	g.initLogger(l, logLevel)

	return g
}
