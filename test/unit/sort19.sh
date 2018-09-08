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
	write dateline="" (name="pear" "value-content-type")
	write dateline="" (name="tomato" "value-literal")
	write dateline="" (name="potato" "attribute-has-rfc-3066-code")
	write dateline="" (name="pepper" "attribute-has-codec")
	read (sort=name name!=null 
		pagesize=2
		result=(cursor (name)))
EOF
rm -rf $D
