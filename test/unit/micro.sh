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
	read id="R00" ()
	read id="R01" (type="T")
	read id="R02" (typeread ("T")
	read id="R03" (typerea 
		())
	read id="R04" (typerea d
		())
	read id="R05" (guid=0000write ("T")
	read id="R06" (guid=0000writ
		())
	read id="R07" (guid=0000writ e
		())
	read id="R08" (value="write ()")
	read id="R09" loglevel=(info read ("T")
	read id="R10" (guid=00000restore ( "X" )
	read id="R11" (typeguid=00000status ( "X" )
	read id="R12" (verify ()
	read id="R13" (restore 
	read id="R14" (restor
		)
	read id="R15" (restor e
		)
	read id="R16" (se )
	read id="R17" (se t
		"T" )
	read id="R18" (s et
		"T" )
	read id="R19" (xset
	read id="R20" (xstseset ()
	read id="R21" ("Person"
		read ()
	read id="R22" ()
EOF
rm -rf $D
