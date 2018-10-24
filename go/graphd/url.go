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

// The url portion of the graphd package implements addressing/url functionality.

package graphd

import (
	"errors"
	"fmt"
	"net/url"
)

// Supported URL schemes over which to connect.
const (
	TCP_SCHEME  = "tcp"
	UNIX_SCHEME = "unix"
)

// Default URL to which to connect.
const (
	DEFAULT_URL_SCHEME = TCP_SCHEME
	DEFAULT_URL_HOST   = "127.0.0.1"
	DEFAULT_URL_PORT   = "8100"
	DEFAULT_URL_STR    = DEFAULT_URL_SCHEME + "://" + DEFAULT_URL_HOST + ":" + DEFAULT_URL_PORT
)

// isURLSchemeSupported checks the URL scheme against supported schemes and returns true or false.
func isURLSchemeSupported(scheme string) bool {
	switch scheme {
	case TCP_SCHEME, UNIX_SCHEME:
		return true
	default:
		return false
	}
}

// parseURLStr takes a URL string of the form
// "scheme://hostname|ipv4_address|[ipv6_address]:port" (where 'port' may either be a port number or
// service name, and returns the url.URL pointer and error obtained from url.Parse.  If the URL scheme
// is not supported, the obtained url.URL pointer is returned along with a new error.
func parseURLStr(urlStr string) (*url.URL, error) {
	// Parse the URL string and check if scheme is supported.
	url, err := url.Parse(urlStr)
	if err == nil && !isURLSchemeSupported(url.Scheme) {
		return url, errors.New(fmt.Sprintf("connections over %v are not supported", url.Scheme))
	}

	return url, err
}

// initURLs tries to parse URL strings in urlStrs to net.URL struct pointers and append them to
// g.urls.  If no URL strings in urlStrs are parsable, or if urlStrs is empty, the default URL is
// parsed.  If this too fails, an error is returned.
func (g *graphd) initURLs(urlStrs []string) error {
	// Try to parse URL strings, skipping failed URLs.
	for _, urlStr := range urlStrs {
		url, err := parseURLStr(urlStr)
		if err != nil {
			g.LogErrf("failed to parse %v: %v", urlStr, err)
			continue
		}
		g.urls = append(g.urls, url)
	}

	// If g.urls is empty, signalling an empty urlStrs or failure to parse any urls, use the default.
	if len(g.urls) == 0 {
		url, err := parseURLStr(DEFAULT_URL_STR)
		if err != nil {
			g.LogErrf("failed to parse default %v: %v", url, err)
			errStr := "failed to parse all urls"
			g.LogErr(errStr)
			return errors.New(errStr)
		}
		g.urls = append(g.urls, url)
	}

	// Return success.
	return nil
}

// GetURLs returns a list of url.URL struct pointers contained within g.urls.
func (g *graphd) GetURLs() []*url.URL {
	return g.urls
}
