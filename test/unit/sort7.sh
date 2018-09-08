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

# Allow variable references on the same level.

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (value="2")
	write (value="1")
	read (any result=$foo $foo=((value)))
	read (any result=(($foo)) $foo=value)
	read (any result=($foo) $foo=(value))
	read (any $foo=$foo)
	read (any $foo=$bar $bar=$baz $baz=$foo)
	read (any $foo=$bar $foo=$baz $baz=(value))
	read (any result=$foo $foo=$bar $foo=$baz $baz=(value) $bar=(value))
	read (sort=$foo $foo=$bar $bar=value)
	read (sort=($foo) $foo=($bar) $bar=value)
	read (result=($foo) $foo=($bar) $bar=(value))
	read (sort=$foo $foo=value result=((value)))
	read (sort=-$foo $foo=value result=((value)))
EOF
rm -rf $D
