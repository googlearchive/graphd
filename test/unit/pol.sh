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

#  Illustrating the examples in the second half of 
#  https://zwiki.metaweb.com/am/ReadQueryResultShapingForFunAndProfit

rm -rf $D
rungraphd -d${D}  -bty <<-'EOF'
	write (type="party" value="Republicans"
		(<-right type="politician" value="Cheney")
		(<-right type="politician" value="Bush")
		(<-right type="politician" value="McCain"))
	write (type="party" value="Democrats"
		(<-right type="politician" value="Lieberman")
		(<-right type="politician" value="Kerry")
		(<-right type="politician" value="Obama"))
	read (type="party" (<-right result=value) result=(value contents))
	read (type="party" (<-right result=((value))) result=(value contents))
	read (type="party" (<-right result=((value))) result=contents)
	read (type="party" (<-right result=((value))) result=((value contents)))
	EOF
rm -rf $D
