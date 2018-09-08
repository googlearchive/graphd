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

# Allow variable references on the same level.

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (type="A" value="3" 
		(<- type="color" name="3 - blue" value="3 value"))
	write (type="A" value="2"
		(<- type="color" name="2 - green" value="2 value"))
	write (type="A" value="1"
		(<- type="color" name="1 - red" value="1 value"))
	read (result=(($x)) type="A" sort=value (<- $x=value))
	read (result=(($x)) type="A" sort=-value (<- $x=value))

	write (type="B" value="34"
		(<- name="3" value="3 value")
		(<- name="4" value="4 value"))
	write (type="B" value="62"
		(<- name="6" value="6 value")
		(<- name="2" value="2 value"))
	write (type="B" value="15"
		(<- name="1" value="1 value")
		(<- name="5" value="5 value"))
	read (type="B" sort=($x) result=((contents))
		(<- $x=((value)) result=((name)) sort=(value)))
	read (type="B" sort=($x) result=((contents))
		(<- $x=value result=((name)) sort=(value)))
EOF
rm -rf $D
