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

# Test vrange-VIP intersects
B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D


# Test integration of inequalities and sorts
# Test null string equavalence (not embedded tabs)
rungraphd -d${D} -bty <<-EOF
 write (value="123"  type="foobar")
 write (value="456"  type="foobar")
 write (value="17"  type="foobar")
 write (value="apple"  type="foobar")
 write (value="snapple"  type="foobar")
 write (value="sharp"  type="foobar")
 write (value="sharpie"  type="foobar")
 write (value="sharpest"  type="foobar")
 write (value="zeebra"  type="foobar")
 write (value="Zeebra"  type="foobar")
 write (value="ZEEBRA"  type="foobar")
 write (value="df-54-12"  type="foobar")
 write (value="df-54-9"  type="foobar")
 write (value=""  type="foobar")
 write (type="foobar")
 write (value="")
 write (value=" ")
 write (value=" 	 	 ")
 write (value="  ")
 write (value="		")

 read (value > "lark" type="foobar" result=((value guid type)))
 read (value~="df" type="foobar" sort=value result=((value guid type)))
 read (value> "df-9" type="foobar" value < "df-90" sort=value result=((value guid type)))
 read (value>"df-100" type="foobar" sort=value result=((value guid type)))
 
 read (value="" result=(guid))
 read (value="               " result=((guid)))
 read (value="				" result=((guid)))
 read (value="	 	 	" result=((guid)))
EOF


rm -rf $D
