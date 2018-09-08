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
	write (value="c" (<-left value="3"))
	write (value="b" (<-left value="2"))
	write (value="a" (<-left value="1"))
	read (sort=$sub result=(($sub value)) (<-left $sub=value))
	read (sort=$sub result=(($sub value))
		   { (<-left value="3" $sub=literal=a) }
		|| { (<-left value="2" $sub=literal=b) }
		|| { (<-left value="1" $sub=literal=c) }
	    )
	read (sort=$sub result=(($sub value))
		   { (<-left value="3") $sub=literal=a }
		|| { (<-left value="2") $sub=literal=b }
		|| { (<-left value="1") $sub=literal=c }
	    )
EOF

rm -rf $D
