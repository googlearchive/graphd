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

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write ()
	write ()
	write ()
	write (-> type="blue" right=00000012400034568000000000000000)
	write (-> type="blue" right=00000012400034568000000000000000)
	write (-> type="blue" right=00000012400034568000000000000002)
	write (-> type="blue" right=00000012400034568000000000000001)
	write (-> type="blue" right=00000012400034568000000000000000)
	write dateline="" (-> type="blue" right=00000012400034568000000000000000)
	write (-> type="blue" right=00000012400034568000000000000001)
	write (-> type="blue" value="chaff")
	write (-> type="blue" value="chaff")
	write (-> type="blue" value="chaff")
	write (-> type="blue" value="chaff")
	write (-> type="blue" value="chaff")
	write (-> type="blue" value="chaff")
	write (-> type="blue" value="chaff")
	write (-> type="blue" value="chaff")
	read (dateline>"123456.12" -> type="blue"
		(guid=(00000012400034568000000000000000 00000012400034568000000000000001 00000012400034568000000000000002)))
	EOF
rm -rf $D

#  con_title = 0x5dc588 
#	(<-left  DATELINE>8a02a8c014b3.9fb TYPEGUID=8a02a8c0400014b380000000000000f6 RESULT=()
#		right->(RESULT=()
#			(<-left RIGHT=8a02a8c0400014b38000000000000055 TYPEGUID=8a02a8c0400014b380000000000000000002 RESULT"..., 
#
# read cost="" dateline="" (optional
#	result=(cursor (guid $blobkey $ctype $lang timestamp))
#	pagesize=500
#	dateline>"8a02a8c014b3.9fb"
#	(-> typeguid=8a02a8c0400014b38000000000000007 right=8a02a8c0400014b380000000000000a2)
#	(-> typeguid=8a02a8c0400014b380000000000000f0 $blobkey=value)
#	(-> typeguid=8a02a8c0400014b380000000000000f6
#		((-> typeguid=8a02a8c0400014b38000000000000002 right=8a02a8c0400014b38000000000000055)
#		 (-> typeguid=8a02a8c0400014b380000000000000ed $ctype=value)))
#	(-> typeguid=8a02a8c0400014b380000000000000ff optional
#		((-> typeguid=8a02a8c0400014b38000000000000002 $lang=value))))
#
