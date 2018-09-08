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

# test "number" sort comparator

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D

# Test the number comparator

rungraphd -d${D} -e 1 -bty <<-'EOF'
 write id="1" (value=".")
 write id="2" (value="0")
 write id="3" (value=" 0")
 write id="4" (value=".0")
 write id="5" (value="0.")
 write id="6" (value="0.0")
 write id="7" (value="0e10")
 write id="8" (value=".e10")
 write id="9" (value="0.e10")
 write id="10" (value=".0e10")
 write id="11" (value="")
 write id="12" (value="1")
 write id="13" (value=" 1")
 write id="14" (value=".1e1")
 write id="15" (value="1.e0")
 write id="16" (value="10e-1")
 write id="17" (value="2.2")
 write id="18" (value="2")
 write id="19" (value="-.")
 write id="20" (value="-0")
 write id="21" (value="-.0")
 write id="22" (value="-0.e2")


 read id="r1" (value="0" result=((value)))
 read id="r2" (value<="0" result=((value)))
 read id="r3" (value>="0" result=((value)))

 read id="r4" (value="0" result=((value)) comparator="number")
 read id="r5" (value<="0" result=((value)) comparator="number")
 read id="r6" (value>="0" result=((value)) comparator="number")

 read id="r7" (value >"1" result=((value)))
 read id="r8" (value ="1" result=((value)))

 read id="r9" (value >"1" result=((value)) comparator="number")
 read id="r10" (value ="1" result=((value)) comparator="number")

 read id="r11" (value="2" result=((value)))
 read id="r12" (value~="2" result=((value)))
 read id="r13" (value>"2" result=((value)))
 read id="r14" (value>="2" result=((value)))

 read id="r15" (value="2" result=((value)) comparator="number")
 read id="r16" (value~="2" result=((value)) comparator="number")
 read id="r17" (value>"2" result=((value)) comparator="number")
 read id="r18" (value>="2" result=((value)) comparator="number")



EOF

