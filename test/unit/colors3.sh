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

# Like colors2, but have the isa sit on top of a sorted
# iterator (VIP, in our case) to test the non-hashtable
# duplicate detection.

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (->((<-())  (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-()) (<-())))
	write (name="teal")
	write (name="mauve")
	write (name="navy")
	write (name="cyan")
	write (name="azure")
	write (name="cerulean")
	write (name="cobalt")
	write (name="indigo")
	write (name="lavender")
	write (name="periwinkle")
	write (name="prussian")
	write (name="ultramarine")
	write (name="root"
	   (<-right type="number" name="1" left=00000012400034568000000000000260)
	   (<-right type="number" name="2" left=00000012400034568000000000000267)
	   (<-right type="number" name="3" left=00000012400034568000000000000261)
	   (<-right type="number" name="4" left=00000012400034568000000000000268)
	   (<-right type="number" name="5" left=00000012400034568000000000000262)
	   (<-right type="number" name="6" left=00000012400034568000000000000269)
	   (<-right type="number" name="7" left=00000012400034568000000000000263)
	   (<-right type="number" name="8" left=0000001240003456800000000000026a)
	   (<-right type="number" name="9" left=00000012400034568000000000000264)
	   (<-right type="number" name="10" left=0000001240003456800000000000026b)
	   (<-right type="number" name="11" left=00000012400034568000000000000265)
	   (<-right type="number" name="12" left=00000012400034568000000000000260))
	read (pagesize=3 result=(cursor (name))
		(-> type="number" (name="root")))
	read (pagesize=3 result=(cursor (name))
		cursor="cursor:8ad7:[o:3]fixed:(fixed-isa:608-620:l<-(vip:630-642:r+00000012400034568000000000000274->620))/3/[cache:@0123456789ab1]"
		(-> type="number" (name="root")))
	read (pagesize=3 result=(cursor (name))
		cursor="cursor:2fd7:[o:6]fixed:(fixed-isa:608-620:l<-(vip:630-642:r+00000012400034568000000000000274->620))/6/[cache:@0123456789ab1]"
		(-> type="number" (name="root")))
	read (pagesize=3 result=(cursor (name))
		cursor="cursor:c0d6:[o:9]fixed:(fixed-isa:608-620:l<-(vip:630-642:r+00000012400034568000000000000274->620))/9/[cache:@0123456789ab1]"
		(-> type="number" (name="root")))
	read (pagesize=3 result=(cursor (name))
		cursor="fixed:(fixed-isa:608-620:left<-(vip:630-642:right+00000012400034568000000000000274->620))/11/[cache:@0123456789ab1]"
		(-> type="number" (name="root")))
	EOF
rm -rf $D
