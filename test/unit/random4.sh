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

# Another one from the random testing.
# You can't have both <-foo and foo= in the same
# constraint.  Not sure why this didn't get caught
# in earlier collision testing...

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
write (type="T")
write ((<-right right=00000012400034568000000000000001))
write ((<-left left=00000012400034568000000000000001))
write ((<-typeguid typeguid=00000012400034568000000000000001))
write ((<-scope scope=00000012400034568000000000000001))
write (right->() right=00000012400034568000000000000001)
write (left->() left=00000012400034568000000000000001)
write (typeguid->() typeguid=00000012400034568000000000000001)
write (scope->() scope=00000012400034568000000000000001)
EOF
rm -rf $D
