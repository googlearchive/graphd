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
	write (name="1" valuetype=1)
	write (name="2" valuetype=2)
	write (name="3" valuetype=3)
	write (name="4" valuetype=4)
	write (name="5" valuetype=5)
	write (name="6" valuetype=6)
	write (name="7" valuetype=7)
	write (name="8" valuetype=8)
	write (name="9" valuetype=9)
	write (name="10" valuetype=10)
	write (name="11" valuetype=11)
	write (name="200" valuetype=200)
	write (name="0" valuetype=0)
	write (name="201" datatype=201)
	write (name="256" datatype=256)
	write (name="string" valuetype=string)
	read (result=((name valuetype datatype)))
	EOF
rm -rf $D
