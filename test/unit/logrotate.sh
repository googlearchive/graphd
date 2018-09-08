# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#!/bin/bash

#
# Emanuele Altieri <emanuele@metaweb.com>
# June 20, 2007
#

#
# This is a basic test for the log rotation mechanism in netlogs and main logs.
#
# The test should generate 4 log files:
#
#    graphd.log.M
#    graphd.log.N
#    graphd.netlog.M
#    graphd.netlog.N
#
# where M and N are two consecutive minutes.
#

B=`basename $0 .sh`
cd `dirname $0`

GFLAGS="-v info"
source ./rungraphd

rm -rf $D graphd.log.* graphd.netlog.*

# start graphd, this generates the first two log files
rungraphd -d$D -f logrotate.conf 2>/dev/null

# sleep until the next minute
# (notice that `now' below has values "00" through "59")
now=`date +%S`
delay=$((160 - 1$now))

# wait for the rest of the current minute
sleep $delay

# kill graphd, this generates the last two log files
rungraphd -d$D -f logrotate.conf -z 2>/dev/null

# count log files
ls -C1 graphd.log.* graphd.netlog.* | wc -l | awk '{print $1}'

# rm -rf $D graphd.log.* graphd.netlog.*
