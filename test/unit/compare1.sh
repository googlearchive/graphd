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
rungraphd -d${D} -bty  <<-'EOF'
	write (value="0")
	write (value="00")
	write (value="000")
	write (value="0.0")
	write (value="0.0000")
	write (value="1.0")
	write (value="0.1.0")
	write (value="01.000")
	read id="0 00 000 0.0 0.0000" (value="0" result=((value)))
	read id="0 00 000 0.0 0.0000" (value="000" result=((value)))
	read id="0 00 000 0.0 0.0000" (value="0.00" result=((value)))
	read id="0 00 000 0.0 0.0000" (value<="0" value>="0" result=((value)))
	read id="0 00 000 0.0 0.0000 0.1.0" (value~="0" result=((value)))
	read id="0" (any comparator="i;octet" value="0" result=((value)))
	read id="000" (comparator="i;octet" value="000" result=((value)))
	read id="1.0 0.1.0 01.000" (value~="*\\0" value~="1" result=((value)))
	read id="1.0 01.000" (value<="1.0" value>="1.0" result=((value)))
	EOF
rm -rf $D
