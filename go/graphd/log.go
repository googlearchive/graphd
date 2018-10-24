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

// The log portion of the graphd package implements a basic logging mechanism.

package graphd

import (
	"fmt"
	"log"
	"log/syslog"
)

var logLevelStrs = map[syslog.Priority]string{
	syslog.LOG_EMERG:   "emergency",
	syslog.LOG_ALERT:   "alert",
	syslog.LOG_CRIT:    "critical",
	syslog.LOG_ERR:     "error",
	syslog.LOG_WARNING: "warning",
	syslog.LOG_NOTICE:  "notice",
	syslog.LOG_INFO:    "info",
	syslog.LOG_DEBUG:   "debug",
}

// logLevelToStr returns a string representation of logLevel, or "unknown".
func logLevelToStr(logLevel syslog.Priority) string {
	if logLevelStr, ok := logLevelStrs[logLevel]; !ok {
		return "unknown"
	} else {
		return logLevelStr
	}
}

// Allows use of any datatype which implements Print, as a logger (ie. a log.Logger tied to
// syslog, or to os.Stderr/Stdout).
type Logger interface {
	Print(v ...interface{})
}

// graphdLogger embeds the logger interface above.  logLevel is used to determine if the log
// message should be emitted.
type graphdLogger struct {
	Logger
	logLevel syslog.Priority
}

// initLogger initializes a new graphdLogger.  If l is nil, default to using syslog with severity
// LOG_ERR faciltiy LOG_USER.  logLevel is used to control whether writes to logger will be emitted.
func (g *graphd) initLogger(l Logger, logLevel syslog.Priority) {
	loggerToUse := l
	var err error
	if loggerToUse == nil {
		loggerToUse, err = syslog.NewLogger(syslog.LOG_ERR|syslog.LOG_USER, 0)
		if err != nil {
			log.Fatalf("failed to initialize default system logger: %v", err)
		}
	}
	g.logger = &graphdLogger{loggerToUse, logLevel}
}

// GetLogLevel returns both a string and numerical representation of the currently set log level.
func (g *graphd) GetLogLevel() (syslog.Priority, string) {
	return g.logger.logLevel, logLevelToStr(g.logger.logLevel)
}

// SetLogLevel sets the log level of graphdLogger to logLevel.
func (g *graphd) SetLogLevel(logLevel syslog.Priority) {
	g.logger.logLevel = logLevel
}

// logMaybe sends s to the logger only if the priority p of the message is less than or equal to the
// log level specified to initLogger().  If emitted, it will be sent with the priority carried by
// the logger interface argument passed to initLogger.
func (g *graphd) logMaybe(p syslog.Priority, s string) {
	if p <= g.logger.logLevel {
		logStr := logLevelToStr(p) + ": " + s
		g.logger.Print(logStr)
	}
}

// LogFatal logs at syslog.LOG_EMERG (highest priority) and follows up with a death call to log.Fatal,
// triggering an os.Exit.
func (g *graphd) LogFatal(s string) {
	logStr := "fatal: " + s
	g.logMaybe(syslog.LOG_EMERG, logStr)
	log.Fatal(logStr)
}

// LogFatalf logs at syslog.LOG_EMERG (highest priority) and follows up with a death call to log.Fatalf,
// triggering an os.Exit.
func (g *graphd) LogFatalf(format string, v ...interface{}) {
	g.LogFatal(fmt.Sprintf(format, v...))
}

// LogEmerg logs at syslog.LOG_EMERG level.
func (g *graphd) LogEmerg(s string) {
	g.logMaybe(syslog.LOG_EMERG, s)
}

// LogEmergf logs at syslog.LOG_EMERG level.
func (g *graphd) LogEmergf(format string, v ...interface{}) {
	g.LogEmerg(fmt.Sprintf(format, v...))
}

// LogAlert tries to log at syslog.LOG_ALERT level.
func (g *graphd) LogAlert(s string) {
	g.logMaybe(syslog.LOG_ALERT, s)
}

// LogAlertf tries to log at syslog.LOG_ALERT level.
func (g *graphd) LogAlertf(format string, v ...interface{}) {
	g.LogAlert(fmt.Sprintf(format, v...))
}

// LogCrit tries to log at syslog.LOG_CRIT level.
func (g *graphd) LogCrit(s string) {
	g.logMaybe(syslog.LOG_CRIT, s)
}

// LogCritf tries to log at syslog.LOG_CRIT level.
func (g *graphd) LogCritf(format string, v ...interface{}) {
	g.LogCrit(fmt.Sprintf(format, v...))
}

// LogErr tries to log at syslog.LOG_ERR level.
func (g *graphd) LogErr(s string) {
	g.logMaybe(syslog.LOG_ERR, s)
}

// LogErrf tries to log at syslog.LOG_ERR level.
func (g *graphd) LogErrf(format string, v ...interface{}) {
	g.LogErr(fmt.Sprintf(format, v...))
}

// LogWarn tries to log at syslog.LOG_WARNING level.
func (g *graphd) LogWarn(s string) {
	g.logMaybe(syslog.LOG_WARNING, s)
}

// LogWarnf tries to log at syslog.LOG_WARNING level.
func (g *graphd) LogWarnf(format string, v ...interface{}) {
	g.LogWarn(fmt.Sprintf(format, v...))
}

// LogNotice tries to log at syslog.LOG_NOTICE level.
func (g *graphd) LogNotice(s string) {
	g.logMaybe(syslog.LOG_NOTICE, s)
}

// LogNoticef tries to log at syslog.LOG_NOTICE level.
func (g *graphd) LogNoticef(format string, v ...interface{}) {
	g.LogNotice(fmt.Sprintf(format, v...))
}

// LogInfo tries to log at syslog.LOG_INFO level.
func (g *graphd) LogInfo(s string) {
	g.logMaybe(syslog.LOG_INFO, s)
}

// LogInfof tries to log at syslog.LOG_INFO level.
func (g *graphd) LogInfof(format string, v ...interface{}) {
	g.LogInfo(fmt.Sprintf(format, v...))
}

// LogDebug tries to log at syslog.LOG_DEBUG level.
func (g *graphd) LogDebug(s string) {
	g.logMaybe(syslog.LOG_DEBUG, s)
}

// LogDebugf tries to log at syslog.LOG_DEBUG level.
func (g *graphd) LogDebugf(format string, v ...interface{}) {
	g.LogDebug(fmt.Sprintf(format, v...))
}
