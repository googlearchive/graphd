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
	write (value="foo")
	write (value="foo" result=none)
	write (value="foo" result=literal="Hello, World!")
	write (value="foo" result=(((literal="*** boing ***"))))
	write (value="foo" result=(guid guid guid))
	write (value="foo" 
		(<- value="bar" result=literal="Hello,")
		(<- value="baz" result=literal="World!")
		result=contents)
	write (value="foo" 
		(<- value="bar" result=literal="Hello,")
		(<- value="baz" result=literal="World!")
		result=(guid contents))
	write (value="foo" 
		(<- value="bar" result=literal="Hello,")
		(<- value="baz" result=literal="World!"))
	write (result=value)
	write ((<- value="bar"))
	write ((<- value="bar" result=none))
	EOF
rm -rf $D
