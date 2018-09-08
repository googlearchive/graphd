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

# Walk tentacle ends of a squid. 

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (value="root")
	write (value="anchor")
	write (right=00000012400034568000000000000000 type="A" left->(
		(<-left type="B" right=00000012400034568000000000000001)))
	write (right=00000012400034568000000000000000 type="A" left->(
		(<-left type="B" right=00000012400034568000000000000001)))
	write (right=00000012400034568000000000000000 type="A" left->(
		(<-left type="B" right=00000012400034568000000000000001)))
	write (right=00000012400034568000000000000000 type="A" left->(
		(<-left type="B" right=00000012400034568000000000000001)))
	write (right=00000012400034568000000000000000 type="A" left->(
		(<-left type="B" right=00000012400034568000000000000001)))
	write (right=00000012400034568000000000000000 type="A" left->(
		(<-left type="B" right=00000012400034568000000000000001)))
	write (right=00000012400034568000000000000000 type="A" left->(
		(<-left type="B" right=00000012400034568000000000000001)))
	read (guid=00000012400034568000000000000000 result=contents
		(<- result=(cursor (guid)) pagesize=2 type="A" left->(
		(<-left type="B" right=00000012400034568000000000000001))))
	read (guid=00000012400034568000000000000000 result=contents
		(<-
			cursor="fixed:7:15,18,21,24,27,30,33/2/"
			result=(cursor (guid))
			pagesize=2 type="A" left->(
		(<-left type="B" right=00000012400034568000000000000001))))
EOF
rm -rf $D
