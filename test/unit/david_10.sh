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


#test variable propogation with sort comparators
B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D

# Test the number comparator

rungraphd -d${D} -e 1 -bty <<-'EOF'
 write id="1" (value="+inf")
 write id="2" (value="Inf")
 write id="3" (value="-inf")
 write id="4" (value="0")
 write id="5" (value="0e100")
 write id="6" (value="-0e5")
 write id="7" (value="infinity")
 write id="8" (value="in")
 write id="9" (value="-7")
 write id="10" (value="60")
 write id="11" (value="-5e4")
 write id="12" (value="-0.005e6")
 write id="13" (value=" 007")
 write id="14" (value="13")
 write id="15" (value="1.3e1")
 write id="16" (value="43.55e2")
 write id="17" (value="4355")
 write id="18" (value="4365")
 write id="19" (value="4345")
 write id="20" (value="14.10")
 write id="21" (value="14.7")
 write id="22" (value="9")
 write id="23" (value="2e999")
 write id="24" (value="hi")
 write id="25" (value="  ")
 write id="26" (value="")
 write id="27" ()
 

 read id="r1" (comparator="number" result=((value)) value>"1e999")
 read id="r2" (comparator="number" result=((value)) value < "0")
 read id="r3" (comparator="number" result=((value)) value>"-inf")
 read id="r3.5" (comparator="number" result=((value)) value>"-inf" sort=value)
 read id="r4" (comparator="number" result=((value)) value>="-inf")
 read id="r5" (comparator="number" result=((value)) value<"-inf")
 read id="r6" (comparator="number" result=((value)) value<="-inf")
 read id="r7" (comparator="number" result=((value)) value<"+inf")
 read id="r8" (comparator="number" result=((value)) value<="inf")
 read id="r9" (comparator="number" result=((value)) value="4.355e3")
 read id="r9.5"(comparator="number" result=((value)) value="43.55e2")
 read id="r10" (comparator="number" result=((value)) value="43.65e2")
 read id="r11" (comparator="number" result=((value)) value="4345")
 read id="r12" (comparator="number" result=((value)) value="0")
 read id="r13" (comparator="number" result=((value)) value<"0.00e45")
 read id="r14" (comparator="number" result=((value)) value > "8" sort=value)
 read id="r15" (comparator="number" result=((value)) sort=value)
 read id="r16" (comparator="number" result=((value)) sort=-value)
 read id="r17" (comparator="number" result=((value)) value <="7" value>"-inf" sort=value)
 read id="r18" (comparator="number" result=((value)) value > "6" value <= "4355")
 read id="r19" (comparator="number" result=((value)) value > "6" value <= "4355" sort=value)
 read id="r20" (comparator="number" value <"10" value > "20")

EOF

