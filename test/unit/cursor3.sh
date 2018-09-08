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
	write (value="27"
		(-> (value="A")))
	write (value="28"
		(-> (value="B")))
	write (value="29"
		(-> (value="C")))
	write (value="30"
		(-> (value="D")))
	write (value="31"
		(-> (value="A")))
	write (value="32"
		(-> (value="B")))
	write (value="33"
		(-> (value="C")))
	write (value="34"
		(-> (value="D")))
	write (value="35"
		(-> (value="A")))
	write (value="36"
		(-> (value="B")))
	write (value="37"
		(-> (value="C")))
	write (value="38"
		(-> (value="D")))
	write (value="39"
		(-> (value="A")))
	write (value="40"
		(-> (value="B")))
	write (value="41"
		(-> (value="C")))
	write (value="42"
		(-> (value="D")))
	write (value="43"
		(-> (value="A")))
	write (value="44"
		(-> (value="B")))
	write (value="45"
		(-> (value="C")))
	write (value="46"
		(-> (value="D")))
	write (value="47"
		(-> (value="A")))
	write (value="48"
		(-> (value="B")))
	write (value="49"
		(-> (value="C")))
	write (value="50"
		(-> (value="D")))
	read (result=(cursor (value)) pagesize=1 (->(value="A")))
	read (result=(cursor (value)) pagesize=1 (->(value="A")) cursor="fixed:6:26,38,50,62,74,86/1/")
	read (result=(cursor (value)) pagesize=1 (->(value="A")) cursor="fixed:6:26,38,50,62,74,86/2/")
	read (result=(cursor (value)) pagesize=1 (->(value="A")) cursor="fixed:6:26,38,50,62,74,86/3/")
	read (result=(cursor (value)) pagesize=1 (->(value="A")) cursor="fixed:6:26,38,50,62,74,86/4/")
	read (result=(cursor (value)) pagesize=1 (->(value="A")) cursor="fixed:6:26,38,50,62,74,86/5/")
	read (result=(cursor (value)) pagesize=1 (->(value="A")) cursor="fixed:6:26,38,50,62,74,86/6/")
EOF
rm -rf $D
