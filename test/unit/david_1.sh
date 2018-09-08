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

#Generate 2000 large files with the max descriptor count set to 300
#and make sure they work

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D

T="david_1.in"
TEST=500
echo -n "" > $T


while ( let "$TEST" ) do
 echo 'write (value="'$TEST'" (<-left) (<-left) (<-left) (<-left) (<-left) (<-left) (<-left) (<-left) (<-left))' >> $T
 let "TEST=$TEST-1"
done

rungraphd -d${D} -f david_1.conf  -bty < $T

rungraphd -d${D} -f david_1.conf -bty <<-EOF
 read ( left->(value="499"))
 read ( left->(value="9"))
EOF
rm -rf $D
