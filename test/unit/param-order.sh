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

( rungraphd -d${D} -bty | sed 's/cost=\"[^"]*\"/cost="omitted"/g' ) <<-'EOF'
	read (optional name="a")
	read cost="" (optional name="b")
	read cost="" cost="" (optional name="c")
	read cost="" id="flipper" dateline="" (optional name="d")
	read cost="" id="flipper" (optional name="e")
	read dateline="" (optional name="f")
	read dateline="" dateline="" id="flipper" cost="" (optional name="g")
	read id="foo" (optional name="h")
	read id="foo" cost="" id="bar" (optional name="i")
	read id="foo" cost="This is not a syntactically correct cost." id="bak" (optional name="j")
EOF
rm -rf $D

