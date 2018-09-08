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
rungraphd -d${D} -bty <<-'EOF'
	write ("d" value="4")
	write ("b" value="2")
	write ("a" value="1")
	write ("c" value="3")
	read (any sort=() pagesize=1 result=cursor)
	read (any sort=() pagesize=1 cursor="cursor:6acd:all:0-19/1/" result=cursor)
	read (any sort=() pagesize=1 cursor="cursor:6ac1:all:0-19/2/" result=cursor)
	EOF
rm -rf $D
