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
	write id="01" (value="2000-01-01" )
	write id="02" (value="-2000")
	write id="03" (value="T12:44")
	write id="04" (value="T17:55")
	write id="05" (value="1999")
	write id="06" (value="2038-05-10")
	write id="07" (value="9999-01-01")
	write id="08" (value="-9998-05-05")
	write id="09" (value="1979-05-05")
	write id="10" (value="2008")
	write id="11" (value="1981-11-05")
	write id="12" (value="-0100")
	write id="13" (value="-0004-11-25")
	write id="14" (value="-0574-10-01")
	write id="15" (value="-0574-05-10")
	write id="16" (value="-1234")
	write id="17" (value="-8123")

	write id="18" (value="2008-05-03")
	write id="19" (value="2008-04-30")
	write id="20" (value="2008-01-01")

	read id="r1" (value-comparator="datetime" value="2008" result=((value)))
	read id="r2" (value-comparator="datetime" value>"9999" result=((value)))
	read id="r3" (value-comparator="datetime" value<"0000"    result=((value)))
	read id="r4" (value-comparator="datetime" value<"-5000" result=((value)))
	read id="r5" (value-comparator="datetime" value<="-9999-05-05" result=((value)))
	read id="r6" (value-comparator="datetime" value>"0000" result=((value)))
	read id="r7" (value-comparator="datetime" value>"2000" result=((value)))
	read id="r8" (value-comparator="datetime" value>"2000" value <"-0100" result=((value)))
	read id="r9" (value-comparator="datetime" value>"1981-11-06" value <"1999-01-01" result=((value)))

	
	read id="r10" (sort-comparator="datetime" pagesize=3 sort=value result=((value)))
	read id="r11" (sort-comparator="datetime" pagesize=3 sort=-value result=((value)))

	               
	read id="r12" (sort-comparator="datetime" value-comparator="datetime" value<"0000" result=((value)) sort=value)
	read id="r13" (sort-comparator="datetime" value-comparator="datetime" value<="9999" result=((value)) sort=-value)
	read id="r14" (sort-comparator="datetime" pagesize=2 value-comparator="datetime" value>"-0574" result=((value)) sort=value)
	read id="r15" (sort-comparator="datetime" pagesize=2 value-comparator="datetime" value<"-0574" result=((value)) sort=-value)
	read id="r16" (sort-comparator="datetime" pagesize=1 value-comparator="datetime" value>"2008" result=((value)) sort=value)

	read id="r17" (value-comparator="datetime" value>"T15" value <"T16" result=((value)))
	read id="r18" (value-comparator="datetime" value>"T12" value <"T13" result=((value)))
	read id="r19" (value-comparator="datetime" value>"T01" value <"T16" result=((value)))

	read id="r20" (sort-comparator="datetime" pagesize=1 value>"2008" sort=value result=((value)))
	read id="r21" (sort-comparator="datetime" pagesize=2 value>"2008" sort=value result=((value)))
	read id="r22" (sort-comparator="datetime" pagesize=4 value>"2008" sort=value result=((value)))
	EOF
rm -rf $D
