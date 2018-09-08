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

#  Unique and ~=
#
#  A unique expression that versions another GUID must be
#  unique only once the record that it versions vanishes.
#
rm -rf $D
rungraphd -d${D} -bty <<- 'EOF'
write (value="top"
	(<-left value="branch A"))
write (value="anchor B")

write (value="chaff"
	(<-right value="chaff" left->(anchor value="anchor B")))

write (value="top"
	(<-left anchor value="branch A")
	(<-right unique=(right left) left->(anchor value="anchor B")))

write (value="top"
	(<-left anchor value="branch A")
	(<-right guid=00000012400034568000000000000005
		unique=(right left) left->(anchor value="anchor B")))
EOF
rm -rf $D
