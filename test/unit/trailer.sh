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

# an optimized access shoulnd't miscount the number of 
# remaining records in the presence dateline constraints.

rm -rf $D
rungraphd -d${D}  -bty <<-'EOF'
	write dateline="" (type="sun"
		(<- value="a" (name="lola"))
		(<- value="b" (name="lola"))
		(<- value="f")
		(<- value="p")
		(<- value="q" (name="lola")))
	write dateline="" (
		typeguid=00000012400034568000000000000007 key=typeguid
		(<- value="s" (name="lola"))
		(<- value="t" (name="lola"))
		(<- value="u" (name="lola"))
		(<- value="v" (name="lola"))
		(<- value="w" (name="lola"))
		(<- value="x" (name="lola"))
		(<- value="y" (name="lola"))
		(<- value="z"))
	read (dateline>"123456.16" result=((value)) <- (name="lola"))
	EOF
rm -rf $D
