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
	write (name="d" value="4")
	write (name="b" value="2")
	write (name="a" value="1")
	write (name="c" value="3")
	read (any pagesize=2 sort=(value name type datatype left
		 right live archival scope timestamp)
		result=(cursor (value)))
	read (any pagesize=2 sort=(value name type datatype left
		 right live archival scope timestamp)
 		result=(cursor (value))
		cursor="sort:s1:2s1:bnd2.nnb1b1nt1970-01-01T00:00:00.0001Zg00000012400034568000000000000001")
	read (any pagesize=2 sort=(value name type datatype left
		 right live archival scope timestamp)
 		result=(cursor (value))
		cursor="sort:s1:4s1:dnd2.--b1b1-t1970-01-01T00:00:00.0000Zg00000012400034568000000000000000")
	EOF
rm -rf $D
