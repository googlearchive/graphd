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

# Some vrange nastyness that has been broken in the past

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D

# Note the embedded tabs in request 4 and 6. 

rungraphd -d${D} -bty <<-EOF
 write id="1" (value="mw1806_propval_bcdefghi")
 read id="2" (value<="mw1806_propval_bcdefghi" result=((guid value)))
 read id="3" (value>="mw1806_propval_bcdefghi" result=((guid value)))
 write id="4" (comparator="octet" value="1729715	Township of Nutley	Civil	NJ	34	Essex	013	404900N	0740929W	40.8167669	-74.1579217					18	Orange" )
 read id="5" (value="1729715 Township of Nutley Civil NJ 34 Essex 013 404900N 0740929W 40.8167669 -74.1579217 18 Orange")
 read (comparator="octet" value="1729715	Township of Nutley	Civil	NJ	34	Essex	013	404900N	0740929W	40.8167669	-74.1579217					18	Orange" )
  
  write id="6"  (value="")
  write id="7"  ()
  read id="8"   (value>"sam")

  write id="9"  (value="   x   ")
  read id="10"  (value="x")

  write id="11" ( value="x   y")
  read id="12"  (value="x")

  write id="13" ( value="   a	b		c	 	  ")
  read id="14"  (value="a	b	c	")

  read id="15"  (value="abc")
  
EOF


rm -rf $D
