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
	write (value="a" (<- value="b") (<-value="a"))

	read (value="a" result=literal="")
	read (value="a" result=(value literal="" value))
	read (value="a" result=(literal="" value))
	read (value="a" result=(value literal=""))
	read (value="a" result=((value literal="" value)))
	read (value="a" result=((value contents)) 
		(<- value="b" result=literal="")
		(<- value="a" result=((value))))

	read (value="a" result=none)
	read (value="a" result=(value none value))
	read (value="a" result=(none value))
	read (value="a" result=(value none))
	read (value="a" result=((value none value)))
	read (value="a" result=((value contents)) 
		(<- value="b" result=none)
		(<- value="a" result=((value))))
	read (value="a" result=((value contents)) 
		(<- value="a" result=((value)))
		(<- value="b" result=none))
	read (value="a" result=((value contents)) 
		(<- value="a" result=none)
		(<- value="b" result=none))
	EOF
rm -rf $D
