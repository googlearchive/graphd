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


# test variable propogation with sort comparators
# Also test that inequalities and sorting work together

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D


rungraphd -d${D}  -bty <<-'EOF'
 write id="1" (value="x")
 write id="2" (value="y")
 set (sync=false)
 write id="3" (value="z")
 write id="4" (value="x")
 set (sync=true)
 write id="5" ("a")
 write id="6" ("b")
 set (sync=false)
 write id="7" ("c")
 write id="8" ("a" value="x")
 read (result=((guid)) sort=timestamp value=("a" "x"))
EOF

rm -rf $D
