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
	write (
		name="1 - third result."
		(<- value="a")
		(<- value="b")
	)
	write (
		name="2 - second and third result"
		(<- value="a")
	)
	write (
		name="3 - first and third result"
		(<- value="b")
	)
	read (left=null right=null (<- value="a" count=0) result=((name)))
	read (left=null right=null (<- value="b" count=0) result=((name)))
	read (left=null right=null (<- value="c" count=0) result=((name)))
	read (left=null right=null (<- value="a" count=0) (<- value="b" count=0) result=((name)))
	write (name="larry" value="only in last" (-> (name="curly" (-> (name="moe")))))
	write (name="larry" value="found (1/2)" (-> (name="curly" (-> (name="moe"))))
			    (-> (name="curly" (-> (name="shemp")))))
	write (name="larry" value="found (2/2)" (-> (name="curly" (-> (name="moe"))))
			    (-> (name="curly" (-> (name="joe")))))
	read (name="larry" (-> (name="curly" (-> count=0 (name="moe")))) result=((value)))
	read (name="larry" (-> count=0 (name="curly" (-> count=0 (name="moe")))) result=((value)))
	EOF
rm -rf $D
