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
	write dateline="" ("picked" value="guitar" name="GR033")
	write dateline="" ("bowed" value="viola" name="GR033")
	write dateline="" ("plucked" value="harp" name="GR033")
	read (dateline>"123456.10" value~="*" name="GR033" result=((value)))
	read (dateline>"123456.a" value~="*" name="GR033" result=((value)))
EOF
rm -rf $D
