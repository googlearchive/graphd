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

# write (node scope=9202a8c04000641f8000000000000000 key=()
#            (<- typeguid=9202a8c04000641f8000000000000002
#                    value="es"
#                    scope=9202a8c04000641f8000000000000000
#                    key=(left right typeguid value)
#                    left=9202a8c04000641f800000000000008e))

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (name="old" key=() (<- name="old" value="foo" key=(right value)))
	write (name="new" key=() (<- name="new" value="foo" key=(right value)))
	read (value="foo" result=(right))
	read (value="foo" right->())
	EOF
rm -rf $D
