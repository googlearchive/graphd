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

# four ways of saying null each with previous and next ("nut" ... "screw")
#
# two writes with non-empty next; they fail.
#
# a previous=1 version ("spring" -> "summer")
# read shows that that worked, returns (("spring"), ("summer"))
#
# two writes with previous=guid and guid=guid; both fail in the
# same way; the previous GUID isn't the most recent in the lineage.
#
# a write with ~= instead of the = works, because the value slides
# down the lineage.  This works for previous~= ("fall") and guid~= ("winter").
#
# Reading with a ~= PREVIOUS value returns only the most recent version;
# throw in a newest>=0 to counteract that and get all but the first.
#
# Reading with a = or ~= NEXT implicitly sets newest>0 if unset;
# if the user sets it, they can restrict further.
# 

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (previous~=null value="nut")
	write (previous=null value="spring")
	write (previous~=() value="bolt")
	write (previous=() value="clamp")
	write (next=null value="nail")
	write (next~=null value="lever")
	write (next=() value="washer")
	write (next~=() value="screw"
	)
	write (next=00000012400034568000000000000001 value="nonempty-next")
	write (next~=00000012400034568000000000000001 value="nonempty-next"
	)
	write (previous=00000012400034568000000000000001 value="summer")
	read (guid~=00000012400034568000000000000001 newest>=0 result=((value))
	)
	write (guid=00000012400034568000000000000001 value="outdated")
	write (previous=00000012400034568000000000000001 value="outdated"
	)
	write (previous~=00000012400034568000000000000001 value="fall")
	write (guid~=00000012400034568000000000000001 value="winter")
	read (guid~=00000012400034568000000000000001 newest>=0 result=((value))
	)
	read (previous~=00000012400034568000000000000001 result=((value)))
	read (previous~=00000012400034568000000000000001 result=((value))
		newest>=0
	)
	read (previous=00000012400034568000000000000001 result=((value)))
	read (previous=00000012400034568000000000000001 result=((value))
		newest>=0
	)
	read (next=00000012400034568000000000000008 result=((value)))
	read (next~=00000012400034568000000000000001 result=((value)))
	read (next~=00000012400034568000000000000001 newest=1 result=((value))
	)
	EOF
rm -rf $D
