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

rm -rf $D $B.pid
rungraphd -d${D} -p${B}.pid -bty << 'EOF'
write (value="avalanche")
write (value="aardvark")
write (value="a3")
write (value="a4")
write (value="a5")
write (value="blubb")
write (value="troet")
write (value="zoppel")
write (value="asterisk")
read (any pagesize=2 value~="a*" result=(cursor (guid value)))
read (any pagesize=2 value~="a*" result=(cursor (guid value))	
	cursor="fixed:6:0,1,2,3,4,8/2/")
read (any pagesize=2 value~="a*" result=(cursor (guid value))	
	cursor="fixed:6:0,1,2,3,4,8/4/")
read (any pagesize=2 value~="a*" result=(cursor (guid value))
	cursor="fixed:6:0,1,2,3,4,8/6/")
EOF
rm -rf $D $B.pid
