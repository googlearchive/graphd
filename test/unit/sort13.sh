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

# Remote recursive sort.

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (value="band"
		(-> (type="record" value="A"
			(-> (type="track" value="A1" 
				(-> type="track-length" value="43")))
			(-> (type="track" value="A2" 
				(-> type="track-length" value="33")))
			(-> (type="track" value="A3"
				(-> type="track-length" value="13")))))
		(-> (type="record" value="B"
			(-> (type="track" value="B1" 
				(-> type="track-length" value="32")))
			(-> (type="track" value="B2" 
				(-> type="track-length" value="52")))
			(-> (type="track" value="B3"
				(-> type="track-length" value="22")))))
		(-> (type="record" value="C"
			(-> (type="track" value="C1" 
				(-> type="track-length" value="10")))
			(-> (type="track" value="C2" 
				(-> type="track-length" value="42")))
			(-> (type="track" value="C3"
				(-> type="track-length" value="52"))))))
	read (type="record" result=((value $length contents)) sort=$length
		(-> sort=$length result=(($length contents))
			(type="track" result=($length value)
			(-> type="track-length" $length=value)))
	)
	read (type="record" result=($x3 (value $x3 contents)) sort=$x3
		(-> $x3=$x2 sort=$x2 result=($x2 ($x2 contents))
			(type="track" $x2=$x1 result=($x1 ($x1 value))
				(-> type="track-length" $x1=value))))
EOF
rm -rf $D
