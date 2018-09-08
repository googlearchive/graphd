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

rm -rf $D
rungraphd -d${D}  -bty <<-'EOF'
	write (value="2")
	write (value="-2")
	write (value="-1")
	write (value="1")
	write (value="0")
	write (value="+0")
	write (value="-0")
	write (value="3")
	read id="a" (value>"1" result=((value)) sort=value)
	read id="b" (value<="1" result=((value)) sort=value)
	read id="c" (value>="1" result=((value)) sort=value)
	read id="d" (value<"1" result=((value)) sort=value)
	read id="e" (value>"-1" result=((value)) sort=value)
	read id="f" (value<="-1" result=((value)) sort=value)
	read id="g" (value>="-1" result=((value)) sort=value)
	read id="h" (value<"-1" result=((value)) sort=value)
	read id="i" (value>"+0" result=((value)) sort=value)
	read id="j" (value<"-0" result=((value)) sort=value)
	read id="k" (value="0" result=((value)) sort=value)
	read id="l" (value>="-0" value<="+0" result=((value)) sort=value)
	write (value="foo 2")
	write (value="foo -2")
	write (value="bar -1")
	write (value="foo 1")
	write (value="baz 0")
	write (value="foo +0")
	write (value="foo -0")
	write (value="foo .0")
	write (value="foo 3")
	read id="m" (value<="foo .0" result=((value)) sort=value)
	read id="m" (value~="foo .0" result=((value)) sort=value)
	read id="m" (value<="foo .0" value~="foo*" result=((value)) sort=value)
	read id="m" (value>="foo .0" value~="foo*" result=((value)) sort=value)
	read id="n" (value>=("1" "0") result=((value)) sort=value)
	read id="n" (value>=() result=((value)) sort=value)
	EOF
rm -rf $D
