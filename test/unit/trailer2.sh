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

# details of system behavior for a dateline constraint on 
# a more deeply nested link.
#
# structure:
#
#  SUN   .-- one(9)----> lola(6)
#  _:_  /
#  |5|-<---- two(A)----> lulu(7)
#       \
#        `-- three(B)--> lila(8)
#

rm -rf $D
rungraphd -d${D}  -bty <<-'EOF'
	write dateline="" (type="sun")
	write dateline="" (name="lola")
	write dateline="" (name="lulu")
	write dateline="" (name="lila")
	write dateline="" (left=00000012400034568000000000000009
			   right=0000001240003456800000000000000A
			   value="one")
	write dateline="" (left=00000012400034568000000000000009
			   right=0000001240003456800000000000000B
			   value="two")
	write dateline="" (left=00000012400034568000000000000009
			  right=0000001240003456800000000000000C
			  value="three")
	write dateline="" (left=00000012400034568000000000000009
			  value="we're on the road to nowhere")
	read id="Aminus" (type="sun" result=contents (-> () result=((value))))
	read id="A" (type="sun" result=contents (-> dateline>"123456.d" () result=((value))))
	read id="B" (type="sun" result=contents (-> dateline>"123456.e" () result=((value))))
	read id="C" (type="sun" result=contents (-> dateline>"123456.f" () result=((value))))
	read id="D" (type="sun" result=contents (-> dateline>"123456.10" () result=((value))))
	read id="E" (type="sun" result=contents (-> dateline>"123456.11" () result=((value))))
	read id="F" (type="sun" result=contents (-> dateline<"123456.11" () result=((value))))
	read id="G" (type="sun" result=contents (-> dateline<"123456.10" () result=((value))))
	read id="H" (type="sun" result=contents (-> dateline<"123456.f" () result=((value))))
	read id="I" (type="sun" result=contents (-> dateline<"123456.e" () result=((value))))
	read id="J" (type="sun" result=contents (-> dateline<"123456.d" () result=((value))))
	EOF
rm -rf $D
