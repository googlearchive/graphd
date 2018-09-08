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
	read (guid=159e048d4000159e8000000012345678)
	read (guid=00000000000000000000000000000000)
	read (guid=0)
	read (guid=null)
	read (any right=159e048d4000159e8000000012345678)
	read (any right=null)
	read (any right=0)
	read (any left=159e048d4000159e8000000012345678)
	read (any left=null)
	read (any left=0)
	read (scope=159e048d4000159e8000000012345678)
	read (scope=null)
	read (scope=0)
	EOF
rm -rf $D
