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
	write (value="1")
	write (value="2")
	write (value="3")
	write (value="4")
	write (value="5")
	write (value="6")
	write (value="7")
	write (value="8")
	write (value="9")
	write (value="10")
	write (value="11")
	write (value="12")
	write (value="13")
	write (value="14")
	write (value="15")
	write (value="16")
	write (value="17")
	write (value="18")
	write (value="19")
	write (value="20")
	write (value="21")
	write (value="22")
	write (value="23")
	write (value="24")
	write (value="25")
	write (value="26")
	read (result=(cursor (value)) pagesize=1)
	read (result=(cursor (value)) cursor="all:0-26/1/" pagesize=1)
	read (result=(cursor (value)) cursor="all:0-26/2/" pagesize=1)
	read (result=(cursor (value)) cursor="all:0-26/24/" pagesize=24)
	read (result=(cursor (value)) cursor="all:0-26/26/" pagesize=24)
EOF
rm -rf $D
