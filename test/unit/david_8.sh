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


# Ensure that value-comparator proporly overrides comparator
B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D


rungraphd -d${D} -bty <<-EOF
 write id="1" (value="2 a")
 write id="2" (value="10")
 read id="r1" (comparator="octet" value="10.0000")
 read id="r2" (value-comparator="default" comparator="octet" value="10.000")
 read id="r3" (value-comparator="octet" sort=(value) result=((value)))
 
EOF


rm -rf $D
