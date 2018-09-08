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
	write dateline="" (value="find me")
	write dateline="" (right=00000012400034568000000000000000 value="anchor")
	read id="mand" (result=value (<- value="anchor"))
	read id="mand<3" (result=value (<- value="anchor" dateline<"123456.3"))
	read id="mand<2" (result=value (<- value="anchor" dateline<"123456.2"))
	read id="mand<1" (result=value (<- value="anchor" dateline<"123456.1"))
	read id="mand<0" (result=value (<- value="anchor" dateline<"123456.0"))
	read id="opt" (result=value (<- value="anchor" optional))
	read id="opt<2" (result=value (<- value="anchor" optional dateline<"123456.2"))
	read id="opt<1" (result=value (<- value="anchor" optional dateline<"123456.1"))
	read id="opt<0" (result=value (<- value="anchor" optional dateline<"123456.0"))
	EOF
rm -rf $D
