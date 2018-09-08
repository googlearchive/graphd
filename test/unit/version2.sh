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
	write ("fish" value="tuna"
		(-> "attribute" value="is-color-of" ("color" value="blue")))
	write ("fish" value="angler"
		(-> "attribute" value="is-color-of" ("color" value="red")))
	read (result=(($fish))
		type="color" value="red"
		(<- "attribute" ("fish" $fish=value)))
	write (-> guid ~= 0000001240003456800000000000000f
		"attribute" value="is-color-of"
		left=00000012400034568000000000000009
		("color" value="red"))
	write (-> guid ~= 00000012400034568000000000000012 
		type="attribute"
		value="is-color-of"
		left=00000012400034568000000000000010
		("color" value="blue"))
	read (result=(($fish))
		type="color" value="red"
		(<- "attribute" ("fish" $fish=value)))
EOF
rm -rf $D
