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
GLD=../../gld/gld

rm -f dateline5.out1
rm -f dateline5.out2
rm -rf $D
rungraphd -d${D} -p${D}.pid -z
rungraphd -d${D} -p${D}.pid -itcp::8111 -bt
echo starting write 1

$GLD -s tcp::8111 -ap <<-'EOF' >> dateline5.out1 && echo write 1: Done. >> dateline5.out1 &
	write dateline="00000012400034568000000000000001" (value="pear")
EOF
sleep 2
$GLD -s tcp::8111 -ap <<'EOF'  >> dateline5.out2 && echo write 2: Done. >> dateline5.out2 &
	write (value="artichoke")
EOF
sleep 2
rungraphd -d${D} -p${D}.pid -z
rm -rf $D
cat dateline5.out1 dateline5.out2
rm -f dateline5.out1
rm -f dateline5.out2
