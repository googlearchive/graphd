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

#make sure that things larger than a single tile can be promoted correctly

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D

T="david_2.in"
TEST=20000

echo -n "" > $T

echo 'write (name="head")' >> $T

while ( let "$TEST" ) do
 echo 'write (value="'$TEST'" left=00000012400034568000000000000000)' >>$T
 let "TEST=$TEST-1"
done

rungraphd -d${D} -bty < $T

rungraphd -d${D} -bty <<-EOF
read (pagesize=64000 left=00000012400034568000000000000000 result=((value)))
EOF

rm $T
rm -rf $D
