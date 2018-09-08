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

# test matching numbers.

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D

# Test the number comparator

rungraphd -d${D} -e 1 -bty <<-'EOF'
write (value="1 2 3")
write (value="04 005 0006")

read (value~="1 * 3" result=((value)))
read (value~="01 * 03" result=((value)))
read (value~="001 * 003" result=((value)))
read (value~="4 * 6" result=((value)))
read (value~="04 * 06" result=((value)))
read (value~="004 * 006" result=((value)))
EOF

rm -rf $D

