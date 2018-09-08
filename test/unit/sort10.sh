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

# test fix for bug #1444.

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

# Allow variable references on the same level.

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (value="sun"
		(<- (value="0"))
		(<- (value="1"))
		(<- (value="2"))
		(<- (value="-1"))
		(<- (value="-2"))
		(<- (value="6")))
	read (value="sun" result=$x (<- pagesize=1 sort=$y $x=((contents)) ($y=value result=value)))
	read (value="sun" result=$x (<- pagesize=2 sort=$y $x=((contents)) ($y=value result=value)))
	read (value="sun" result=$x (<- pagesize=3 sort=$y $x=((contents)) ($y=value result=value)))
	read (value="sun" result=$x (<- pagesize=4 sort=$y $x=((contents)) ($y=value result=value)))
	read (value="sun" result=$x (<- pagesize=5 sort=$y $x=((contents)) ($y=value result=value)))
	read (value="sun" result=$x (<- pagesize=6 sort=$y $x=((contents)) ($y=value result=value)))
EOF
rm -rf $D
