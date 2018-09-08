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

#  In the second write, the node "toplevel" is pointed to
#  by a matched key cluster, and already has its desired values.
#  Therefore, it is not created, even though it itself is not keyed.
#
rm -rf $D
rungraphd -d${D} -bty <<- 'EOF'
	write (name="namespace" (<-left value="foo" right->(name="toplevel")))
	write (name="toplevel" 
		(<-right value="foo" key=(left value)
			left=00000012400034568000000000000000))
EOF
rm -rf $D
