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
	write (name="a")
	write (name="b")
	write (name="c")
	write (name="d")
	read (pagesize=1 result=((name)))
	read (start=0 pagesize=1 result=name)
	read (start=1 pagesize=1 result=(name))
	read (start=2 pagesize=1 result=((name)))
	read (start=3 pagesize=1 result=(count (name)))
	write (name="sun"
		(<-left name="link" right->(value="a"))
		(<-left name="link" right->(value="b"))
		(<-left name="link" right->(value="c"))
		(<-left name="link" right->(value="d")))
	read (name="sun" result=contents (<-left start=0 pagesize=1 result=contents right->(result=value)))
	read (name="sun" result=contents (<-left start=1 pagesize=1 result=((contents)) right->(result=value)))
	read (name="sun" result=contents (<-left start=2 pagesize=1 result=contents right->(result=value)))
	read (name="sun" result=contents (<-left start=3 pagesize=1 result=contents right->(result=value)))
	read (name="sun" result=contents (<-left start=3 pagesize=1 result=count))
EOF
rm -rf $D
