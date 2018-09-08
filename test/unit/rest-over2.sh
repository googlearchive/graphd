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
restore ("2" 0 4 (00000000000000000048d15800000000 null null string "foo" 0 true true 1970-01-01T00:00:00.0000 0 0 0) (00000000000400000048d15800000001 null null string "bar" 0 true true 1970-01-01T00:00:00.0001 0 0 0) (00000000000800000048d15800000002 null null string "foobar" 0 true true 1970-01-01T00:00:00.0002 00000000000000000048d15800000000 00000000000400000048d15800000001 0) (00000000000c00000048d15800000003 null null string "barfoo" 0 true true 1970-01-01T00:00:00.0003 00000000000400000048d15800000001 00000000000000000048d15800000000 0))
restore ("2" 1 6  (00000000000400000048d15800000001 null null string "bar" 0 true true 1970-01-01T00:00:00.0001 0 0 0) (00000000000800000048d15800000002 null null string "foobar" 0 true true 1970-01-01T00:00:00.0002 00000000000000000048d15800000000 00000000000400000048d15800000001 0) (00000000000c00000048d15800000003 null null string "barfoo" 0 true true 1970-01-01T00:00:00.0003 00000000000400000048d15800000001 00000000000000000048d15800000000 0) (00000000001000000048d15800000004 00000000000000000048d15800000000 null string "baz" 0 true true 1970-01-01T00:00:00.0004 0 0 0) (00000000001400000048d15800000005 null "bazooka" null null 00000000000000000048d15800000000 true true 1970-01-01T00:00:00.0005 0 0 0))
EOF
rm -rf $D


