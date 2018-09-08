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
rungraphd -d${D} -bty <<-EOF
	write (value="abcdabcdef" name="1")
	write (value="fOO BaR baZ" name="2")
	write (value="fOOnitzbaZ" name="3")
	write (value="fOO/baZ" name="4")
	write (value="foobar" name="5")
	write (value="arbitrary ,-=(\")[]@~ punctuation " name="6")
	write (value="foot" name="7")
	write (value="pfoo" name="8")
	write (value="pfoot" name="9")
	write (value="yes: (-:*" name="10")
	write (value="no: (--:*" name="11")
	read (value ~= "bAr" result=((value name)))
	read (value ~= "foo-bar" result=((value name)))
	read (value ~= "^bar" result=((value name)))
	read (value ~= "^FOO" result=((value name)))
	read (value ~= "bar" result=((value name)))
	read (value ~= "bar$" result=((value name)))
	read (value ~= "baz$" result=((value name)))
	read (value ~= "arbitrary punctuation" result=((value name)))
	read (value ~= "foo" result=((value name)))
	read (value ~= "foo*" result=((value name)))
	read (value ~= "*foo" result=((value name)))
	read (value ~= "*foo*" result=((value name)))
	read (value ~= "foo * baz" result=((value name)))
	read (value ~= "foo*baz" result=((value name)))
	read (value ~= "\\(\\-\\:\\*" result=((value name)))
EOF
rm -rf $D
