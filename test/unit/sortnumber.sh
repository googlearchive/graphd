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

rungraphd -d${D} -e 1  -bty <<-'EOF'
 write id="00" ()
 write id="01" (value="abc" left=00000012400034568000000000000000 "type-1")
 write id="02" (value="hello" left=00000012400034568000000000000000 "type-2")
 write id="03" (value="" left=00000012400034568000000000000000 "type-1")
 write id="04" ("type-1" left=00000012400034568000000000000000)
 write id="05" (value="-4345.654e55" left=00000012400034568000000000000000 "type-2")
 write id="06" (value="-434.654e56" left=00000012400034568000000000000000 "type-1")
 write id="07" (value="-12e5" left=00000012400034568000000000000000 "type-2")
 write id="08" (value="-1.2e6" left=00000012400034568000000000000000 "type-1")
 write id="09" (value="-0.123e8" left=00000012400034568000000000000000 "type-2")
 write id="10" (value="-4e-3" left=00000012400034568000000000000000 "type-1")
 write id="11" (value="-4e-2" left=00000012400034568000000000000000 "type-2")
 write id="12" (value="-3e-3" left=00000012400034568000000000000000 "type-1")
 write id="13" (value="-3e-2" left=00000012400034568000000000000000 "type-2")
 write id="14" (value="-.03e-1" left=00000012400034568000000000000000 "type-2")
 write id="15" (value=".1" left=00000012400034568000000000000000 "type-1")
 write id="16" (value="3.1e-4" left=00000012400034568000000000000000 "type-2")
 write id="17" (value="3.01e-3" left=00000012400034568000000000000000 "type-1")
 write id="18" (value="3.001e-5" left=00000012400034568000000000000000 "type-2")
 write id="19" (value="3" left=00000012400034568000000000000000 "type-1")
 write id="20" (value="21" left=00000012400034568000000000000000 "type-2")
 write id="21" (value="2.0e1" left=00000012400034568000000000000000 "type-1")
 write id="22" (value="1e-123123123123123123123" left=00000012400034568000000000000000 "type-2")
 write id="23" (value="00.0e0" left=00000012400034568000000000000000 "type-1")
 write id="24" (value="12e0" left=00000012400034568000000000000000 "type-2")
 write id="25" (value="11" left=00000012400034568000000000000000 "type-1")
 write id="26" (value="13" left=00000012400034568000000000000000 "type-2")
 write id="27" (value="13e5" left=00000012400034568000000000000000 "type-1")
 write id="28" (value="54" left=00000012400034568000000000000000 "type-2")
 write id="29" (value="55.34" left=00000012400034568000000000000000 "type-1")
 write id="30" (value="5.0e1" left=00000012400034568000000000000000 "type-2")
 write id="31" (value="54.012" left=00000012400034568000000000000000 "type-1")
 write id="32" (value="5.493e1" left=00000012400034568000000000000000 "type-2")
 write id="33" (value="6e1" left=00000012400034568000000000000000 "type-1")
 write id="34" (value="6.2e32" left=00000012400034568000000000000000 "type-2")

 read id="r1" (result=((value)) sort=value)
 read id="r2" (result=((value)) comparator="numeric")
 read id="r3" (result=((value)) sort=value comparator="numeric")
 read id="r3.5" (result=((value)) sort=value comparator="numeric" pagesize="4")
 read id="r4" (result=((value)) value > "0" value < "1" sort=value comparator="number" pagesize=3)
 read id="r5" (result=((value)) value > "0" value < "1" comparator="number")
 read id="r6" (result=((value)) sort=value comparator="numeric" "type-1" pagesize=2)
 read id="r7" (result=((value)) sort=value comparator="numeric" "type-2" value <="50" value >"-inf" pagesize=2)
EOF

