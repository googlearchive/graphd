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
		(<- (datatype="timestamp" value="2006-11-05"))
		(<- (datatype="timestamp" value="28"))
		(<- (datatype="timestamp" value="T28"))
		(<- (datatype="timestamp" value="1983-02-30")))
	read (value="sun" result=contents
		(<- result=((contents)) (value>="0" value<="99" datatype="timestamp" result=(guid value))))
EOF
rm -rf $D
