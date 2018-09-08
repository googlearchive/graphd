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
	write id="02" (value="-2000-01-01" )
	write id="03" (value="1234-01-01" )
	write id="04" (value="5432-01-01" )
	write id="05" (value="1999-02-01" )
	write id="06" (value="-2000-02-01" )
	write id="07" (value="1432-02-01" )
	write id="08" (value="0003-05-04" )
	write id="09" (value="7012-05-04" )
	write id="10" (value="1234-05-04" )
	write id="11" (value="1000-02-03" )
	write id="12" (value="1000-03-02" )
	write id="13" (value="1000-05-05" )
	write id="14" (value="1001-02-03T12:43" )
	write id="15" (value="2000-01-02T11:34:10.12" )
	write id="16" (value="-4321-09-10" )
	write id="17" (value="-4322-09-10" )
	write id="18" (value="1978-11" )
	write id="19" (value="1978-11-21" )

	read id="r1" (value-comparator="datetime" value~="*-01-01" result=((value)))
	read id="r2" (value-comparator="datetime" value~="*-02-*" result=((value)))
	read id="r3" (comparator="datetime" sort=value value~="*-05-04" result=((value)))
	read id="r4" (value-comparator="datetime" value~="1000-*-03" result=((value)))
	read id="r5" (value-comparator="datetime" value~="1000-02-*" result=((value)))
	read id="r6" (comparator="datetime" sort=value value~="*-*-10" result=((value)))
	read id="r7" (value-comparator="datetime" value~="*-02-03" result=((value)))
	read id="r8" (comparator="datetime" sort=-value value~="*-02-03" result=((value)))
	read id="r9" (comparator="datetime" sort=-value value~="*-11-*" result=((value)))
	read id="r10" (comparator="datetime" value~="-4321-*-10" result=((value)))
	EOF
rm -rf $D
