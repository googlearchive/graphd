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

# Nested sorts.

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (value="grandma"
		(<- value="mom"
			(<- type="kid" value="8")
			(<- type="kid" value="3")
		)
		(<- value="dad"
			(<- type="kid" value="9")
			(<- type="kid" value="4")
			(<- type="kid" value="1")
		)
		(<- value="auntie"
			(<- type="kid" value="5")
		)
	)
	read (value="grandma" result=((contents))
		(<- result=((value contents)) sort=(-$age) (<- type="kid" sort=(-$age) $age=value result=((value)))))
EOF
rm -rf $D
