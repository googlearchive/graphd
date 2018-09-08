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
	write dateline="" (name="pear")
	write dateline="" (name="tomato")
	write dateline="" (name="potato")
	write dateline="" (name="pepper")
	write dateline="" (name="apple")
	write dateline="" (name="onion")
	write dateline="" (name="corn")
	read dateline="it doesn't matter what you put here?" ()
	read (dateline="123456.7" value="No = with dateline in constraints")
	write (dateline > "123456.7" value="No dateline in write requests!")
	read (dateline>"This is not a syntactically correct dateline!")
	read (dateline>123456.3 result=((name)))
	read (dateline>123456.0 result=((name)))
	read (dateline>123456.6 result=((name)))
	read (dateline>123456.7 result=((name)))
	read (dateline>123456.8 result=((name)))
EOF
rm -rf $D
