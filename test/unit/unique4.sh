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

#  Unique and ~=
#
#  A unique expression that versions another GUID must be
#  unique even before the record that it versions becomes
#  invalid.  (The opposite is hard to implement - if I had
#  a good way of implementing it, I'd do it.)
#
#  If that bugs you, and if what you're really doing is just
#  rewriting or adding something, you may want to look into
#  using key= instead of unique= .
#
rm -rf $D
rungraphd -d${D} -bty <<- 'EOF'
write("x" value="Foo")
write(value="Bar")
read (	value="Bar"
	datatype=string 
	(<-left
		datatype=string
		typeguid=00000012400034568000000000000001
		right->(value="Foo" datatype=string)))
write (
	value="Bar"
	datatype=string key=(value)
	(<-right
		value="baz"
		datatype=string
		type="has_key"
		unique=(typeguid right left)
		left->(
			value="Foo"
			datatype=string
			key=(value))
	)
)
write (
	value="Bar" datatype=string unique=(value)
	(<-left
		value="baz"
		datatype=string
		typeguid=00000012400034568000000000000002
		unique=(typeguid right left)
		right->(
			value="Foo"
			datatype=string
			unique=(value))
	)
)
write (
	value="Bar"
	datatype=string
	key=(value)
	(<-left
		value="baz"
		datatype=string
		type="has_name"

	right->(value="Foo"
		datatype=string
		key=(value) ) ) )
write (
	type="boo"
	key=(typeguid))
write (
	type="boo"
	key=(typeguid))
EOF
rm -rf $D
