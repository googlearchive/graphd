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

# test the "octet" comparator

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

rm -rf $D
rungraphd -d${D} -bty <<-EOF
	write (value="foo" name="1")
	write (value="FOO" name="2")
	read (value="foo" result=((value name)))
	read (value="foo" comparator="i;octet" result=((value name)))
	read (value="Foo" comparator="octet" result=((value name)))
	read (value="FOO" comparator="mesopotamian;octet" result=((value name)))
	read (value~="^foo" comparator="octet" result=((value name)))
	read (value~="^foo" comparator="default" result=((value name)))
	write (unique=value value="Foo")
	write (unique=value comparator="octet" value="foo")
	write (unique=value comparator="octet" value="Foo")
	write (key=value value="foo")
	write (key=value comparator="octet" value="FOO")
	write (key=value comparator="octet" value="foO")
EOF
rm -rf $D
