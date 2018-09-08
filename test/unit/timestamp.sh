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

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

#	$ts > 2005-05-06T02:20:34.0009,

rm -rf $D $B.pid
rungraphd -d${D} -bty <<-'EOF'
write (name="foo")
write (name="bar")
read (result=((timestamp)))
read (result=((timestamp)) timestamp < 1970-01-01T00:00:00.0001)
read (result=((timestamp)) timestamp <= 1970-01-01T00:00:00.0000)
read (result=((timestamp)) timestamp = 1970-01-01T00:00:00.0001)
read (result=((timestamp)) timestamp >= 1970-01-01T00:00:00.0001)
read (result=((timestamp)) timestamp > 1970-01-01T00:00:00.0000)
write (name="quux" timestamp=1970-01-02T03:04:05.0006)
read (result=((timestamp)) name="quux")
write (name="quux" timestamp=190-01-02T03:04:05.0006)
EOF
rm -rf $D $B.pid
