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
rungraphd -d${D} -I zork -bty <<-'EOF'
        write (value = "abc")
	write (value="xyz")
	read dateline="noobasdf,00000012400034568000000000000009" (result=((guid)))
	read dateline="zork,00000012400034568000000000000001" (result=((guid)))
	status (instance-id)
	set (instance-id="palm")
	read dateline="zork,00000012400034568000000000000001" (result=((guid)))
	read dateline="00000012400034568000000000000001" (result=((guid)))
	EOF

rungraphd -f instance-id.conf -d${D} -I zork -bty <<-'EOF'
        status (instance-id)
	EOF

rungraphd -f instance-id.conf -d${D} -bty <<-'EOF'
        status (instance-id)
	set (instance-id="")
	status (instance-id)
	read dateline="00000012400034568000000000000001" (result=((guid)))
	EOF

rm -rf $D
