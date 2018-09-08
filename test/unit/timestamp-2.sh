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

# We're building timestamps that are successively
# juuust less than 1980-02-02T:01:01:01.00001.  As
# the date gets more and more specific, fewer and
# fewer timestamps match.
# 

rm -rf $D $B.pid
rungraphd -d${D} -bty <<-'EOF'
write (name="a" timestamp=1970-01-02T01:01:01.0001)
write (name="b" timestamp=1980-01-02T01:01:01.0001)
write (name="c" timestamp=1980-02-01T01:01:01.0001)
write (name="d" timestamp=1980-02-02T00:01:01.0001)
write (name="e" timestamp=1980-02-02T01:00:01.0001)
write (name="f" timestamp=1980-02-02T01:01:00.0001)
write (name="g" timestamp=1980-02-02T01:01:01.0000)
write (name="h" timestamp=1980-02-02T02:01:01.0001)
read (result=((name)))
read (result=((name)) sort=(timestamp) timestamp >= 1980)
read (result=((name)) sort=(timestamp) timestamp >= 1980-02)
read (result=((name)) sort=(timestamp) timestamp >= 1980-02-02)
read (result=((name)) sort=(timestamp) timestamp >= 1980-02-02T01)
read (result=((name)) sort=(timestamp) timestamp >= 1980-02-02T01:01)
read (result=((name)) sort=(timestamp) timestamp >= 1980-02-02T01:01:01)
read (result=((name)) sort=(timestamp) timestamp >= 1980-02-02T01:01:01.0001)
EOF
rm -rf $D $B.pid
