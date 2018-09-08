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

#  The carrot is identified as a vegetable,
#  even though its type name resides in a namespace
#  other than the global one.

rm -rf $D
rungraphd -d${D}  -bty <<-'EOF'
	write (type="animal")
	write (name="my-namespace" (-> "has_key" value="vegetable" (name="type-vegetable" (<-typeguid value="carrot"))))
	read (value="carrot")
	EOF
rm -rf $D
