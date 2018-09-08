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


#Some nasty cases that have crashed the default comparator
# Note the non ascii characters in the values...

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D


rungraphd -d${D} -bty <<-'EOF'
write id="1" (value="a")
write id="2" (value="bob")
write id="3" (value="ZZ")
write id="4" (value="cab")
write id="5" (value="caboç")
write id="6" (value="cab È")
write id="7" (value="cab Aa")

read id="r1" (value~="Aa")
read id="r2" (value <"Zé")
read id="r3" (value >"U" value<"Zé")
EOF

rm -rf $D
