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
	write dateline="" (value="pear")
	write dateline="" (value="tomato")
	write dateline="" (value="potato")
	write dateline="" (value="pepper")
	write dateline="" (value="onion")
	read (sort=value value!=null 
		pagesize=2
		result=(cursor (value)))
	write dateline="" (value="apple")
	write dateline="" (value="corn")
	read (sort=value value!=null 
		pagesize=2
		result=(cursor (value))
		cursor="sort:[o:2][n:5]s4:pearg00000012400034568000000000000000")
	read (sort=value value!=null 
		pagesize=2
		result=(cursor (value))
		cursor="sort:[o:4][n:5]s6:potatog00000012400034568000000000000002")
EOF
rm -rf $D
