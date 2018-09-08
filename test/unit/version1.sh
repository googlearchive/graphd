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
	write ("Hello, World!" name="x" value="1234")
	read (name="x")
	write (guid ~= 00000012400034568000000000000009 "Hello, Moon!" name="x")
	read (name="x")
	write (guid ~= 00000012400034568000000000000009 "Hello, Venus!" name="x")
	read (name="x")
	write (guid ~= 00000012400034568000000000000009 "Hello, Mars!" name="x")
	read (name="x")
	read (oldest=0 newest>=0 name="x")
	read (oldest=1 newest>=0 name="x")
	read (newest=1 name="x")
	read (newest=0 name="x")
	read (guid ~= 00000012400034568000000000000009 result=type name="x")
EOF
rm -rf $D
