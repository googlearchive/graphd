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
	status (cost)
	set (cost="dr=1 dw=2 tu=3 tr=4 ts=5 va=6 ir=7 iw=8 in=9 pf=10 pr=11 fm=12 mm=13 ft=14 mt=15")
	status (cost)
	set (cost="dr=1 tu=3 ts=5 ir=7 in=9 pr=11 mm=13 mt=15")
	status (cost)
	set (cost="falafel=$3.50")
	status (cost)
	EOF
rm -rf $D
