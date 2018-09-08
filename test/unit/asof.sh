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
	write dateline="" (name="john")
	write dateline="" (name="paul")
	write dateline="" (name="ringo")
	write dateline="" (name="george")
	read (any)
	read asof=1970-01-01T00:00:00.0000 (any result=((guid)))
	read asof=1970-01-01T00:00:00.0000 (any optional result=((guid)))
	read asof=00000012400034568000000000000000 (any result=((guid)))
	read asof="123456.1" (any result=((guid)))
	read asof=1970-01-01T00:00:00.0001 (any result=((guid)))
	read asof=00000012400034568000000000000001 (any result=((guid)))
	read asof="123456.2" (any result=((guid)))
	read asof=1970-01-01T00:00:00.0002 (any result=((guid)))
	read asof=00000012400034568000000000000002 (any result=((guid)))
	read asof="123456.3" (any result=((guid)))
	read asof=1970-01-01T00:00:00.0003 (any result=((guid)))
	read asof=00000012400034568000000000000003 (any result=((guid)))
	read asof="123456.4" (any result=((guid)))
	read asof=1970-01-01T00:00:00.0004 (any result=((guid)))
	read asof=1970-01-01T01:01:01.0004 (any result=((guid)))
	write (guid=00000012400034568000000000000000 live=false)
	write dateline="" (guid~=00000012400034568000000000000002 name="the real ringo")
	write (guid=00000012400034568000000000000002 name="ringo")
	read asof=00000012400034568000000000000003 (any result=((guid name)))
	read (any result=((guid name)))
	write (guid=00000012400034568000000000000000 name="john" live=true)
	write (guid~=00000012400034568000000000000000 name="john" live=true)
	EOF
rm -rf $D
