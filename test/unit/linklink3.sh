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
	write (name="target, the fluffy bunny")
	write (-> name="a" left=00000012400034568000000000000000 right=null (-> name="b"))
	write (-> name="c" left=00000012400034568000000000000000 (-> name="d" left=00000012400034568000000000000000))
	write (-> name="e" left=00000012400034568000000000000000 (<- name="f" left=00000012400034568000000000000000))
	write (-> name="g" left=00000012400034568000000000000000 right=null (<- name="h" left=00000012400034568000000000000000))
	write (<- name="i" left=00000012400034568000000000000000 (-> name="j"))
	write (<- name="k" (-> name="l" left=00000012400034568000000000000000))
	write (<- name="m" (<- name="n" left=00000012400034568000000000000000))
	write (<- name="o" left=00000012400034568000000000000000 (<- name="p" left=00000012400034568000000000000000))
	read (any result=((meta name guid left right)))
	EOF
rm -rf $D
