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
	write ("1" value="abcdabcdef")
	write ("2" value="fOO BaR baZ")
	write ("3" value="fOOnitzbaZ")
	write ("4" value="fOO/baZ")
	write ("5" value="foobar")
	write ("6" value="arbitrary ,-=(\")[]@~ punctuation ")
	write ("7" value="foot")
	write ("8" value="pfoo")
	write ("9" value="pfoot")
	write ("10" value="yes: (-:*")
	write ("11" value="no: (--:*")
	read (value ~= "bAr" result=((value type)))
	read (value ~= "foo-bar" result=((value type)))
	read (value ~= "^bar" result=((value type)))
	read (value ~= "^FOO" result=((value type)))
	read (value ~= "bar" result=((value type)))
	read (value ~= "bar$" result=((value type)))
	read (value ~= "baz$" result=((value type)))
	read (value ~= "arbitrary punctuation" result=((value type)))
	read (value ~= "foo" result=((value type)))
	read (value ~= "foo*" result=((value type)))
	read (value ~= "*foo" result=((value type)))
	read (value ~= "*foo*" result=((value type)))
	read (value ~= "foo * baz" result=((value type)))
	read (value ~= "foo*baz" result=((value type)))
	read (value ~= "\\(\\-\\:\\*" result=((value type)))
	read (value ~="bar" value~="foo"  result=((value name)))
	read (value ~="*bar*" value~="*foo*"  result=((value name)))
EOF
rm -rf $D
