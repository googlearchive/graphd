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


# test variable propogation with sort comparators
# Also test that inequalities and sorting work together
B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D


rungraphd -d${D} -e 1 -bty <<-'EOF'
 write id="1" ( value="X" left->( value="Y" left->( value="0000 apple")))
 write id="2" ( value="X" left->( value="Y" left->( value="000 barbie")))
 write id="3" ( value="X" left->( value="Y" left->( value="00 chair")))
 write id="4" ( value="X" left->( value="Y" left->( value="0 david")))
 
 write id="5" ( value="Z" left->( value="Y" left->( value="0000 10")))
 write id="6" ( value="Z" left->( value="Y" left->( value="000 010.00")))
 write id="7" ( value="Z" left->( value="Y" left->( value="00 10.0")))
 write id="8" ( value="Z" left->( value="Y" left->( value="0 010.0")))
 write id="9" ( value="Z" left->( value="Y" left->( value="00000 210.0")))
 write id="10"( value="Z" left->( value="Y" left->( value="000000 012.0")))

 read id="r1" ( result =((guid $b)) value="X" sort=$b left->(value="Y" sort=$a
 		$b = $a left->($a = value)))

 read id="r2" ( result=((guid $b)) sort-comparator=("case") value="X" sort=$b
 		left->(value="Y" sort=$a $b = $a left->($a = value))) 
 read id="r3" ( result=((guid $b)) sort-comparator=("case") value="Z" sort=-$b
 		left->($b = $a left->($a = value value="0 10"))) 

 read id="r4" (result=((guid value)) sort=value  value >"1")
 read id="r5" (result=((guid value)) sort=value  sort-comparator="case" value-comparator="case" value >"1")
 read id="r6" (result=((guid value)) sort=value  comparator="case" value >"1")
EOF

rm -rf $D
