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

rm -rf ${D}

# a variable that's used, but never set.

rungraphd -d${D} -bty <<-'EOF'
	read (result=$foo)
	EOF

# a variable that's set, but never used.

rungraphd -d${D} -bty <<-'EOF'
	read ($foo=value)
	EOF

# a variable that's set twice

rungraphd -d${D} -bty <<-'EOF'
	read (result=$foo ($foo=value $foo=name))
	EOF

rm -rf ${D}
