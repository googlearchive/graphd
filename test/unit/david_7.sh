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

# Test value sorts with different comparator functions

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D


rungraphd -d${D} -bty <<-'EOF'
 write id="1" (value="1"  left->(value="Aa"))
 write id="2" (value="2"  left->(value="aa"))
 write id="3" (value="17" left->(value="ab"))
 write id="4" (value="02" left->(value="Ab"))
 write id="5" (value="1"  left->(value="AB"))
 read  id="6" (result=((value $f)) sort=(value $f) sort-comparator=("default" "octet") 
 	left->( $f = value))
 read  id="7" (result=((value $f)) sort=(value $f) sort-comparator=("default" "default")
 	left->( $f = value))
 read  id="8" (result=(($f value)) sort=($f value) sort-comparator=("octet" "octet")
 	left->( $f = value))
 read  id="9" (result=((value $f)) comparator="octet"  sort=(value $f) 
	sort-comparator="default" left->( $f = value))
 read  id="10" (sort=value sort-comparator="case" result=((value)))
 read  id="11" (sort=value sort-comparator=("case" "case"))
 read  id="12" (sort=value sort-comparator="case" sort-comparator="case")
 
EOF


rm -rf $D
