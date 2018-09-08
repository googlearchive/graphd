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

# Don't lose parser synchronization when hitting
# conflicting constraints.

rm -rf $D
rungraphd -d${D}  -bty <<-'EOF'
	read loglevel=(fatal) (
		LeFT=(adf6ebbf4000ef3e80000003dcdafcab f12ab53b4000e2bf80000002dcdabbad)
		left=null
		value="\t")
	EOF
rm -rf $D
