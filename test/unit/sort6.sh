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

# Bug courtesy of Tim Sturges.

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (value="value 1" (<-value="tag 2"))
	write (value="value 2" name="name 1" (<-))
	write (name="name 2" (<-value="tag 1"))
	read (any result=(($tag name value)) sort=(name)  (<- $tag=value))
	read (any result=(($tag name value)) sort=(value) (<- $tag=value))
	read (any result=(($tag name value)) sort=($tag)  (<- $tag=value))
EOF
rm -rf $D
