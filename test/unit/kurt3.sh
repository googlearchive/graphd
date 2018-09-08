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
  write ("user" value="u1"
	(-> "attribute" name="name1" value="value1")
	(-> "attribute" name="name2" value="value2")
	(-> "attribute" name="name3" value="value3")
	(-> "is-attribute-of" (name="name4" value="value4"))
	(-> "is-attribute-of" (name="name5" value="value5"))
	(-> "is-attribute-of" (name="name6" value="value6")))
 read (value="u1" result=contents
	(-> "attribute" optional result=((name value)))
	(-> "is-attribute-of" result=((contents)) (result=(name value))))
EOF
rm -rf $D
