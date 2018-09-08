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
	ok ((("127.0.0.1:53576" "read" "RUN" 2007-05-31T00:11:10.0000Z 2007-05-31T00:11:44.0000Z 229700 50164 100 4 31166 "read id=\"client;unagi:8105;4327;2006-12-22T02:12:34Z;0001\" (right=null result=((guid timestamp contents)) pagesize=10 left=null scope->(right=null result=((guid contents)) optional pagesize=100 left=null (<-left typeguid=9202a8c04000641f800000000000000c result=((guid typeguid contents)) optional pagesize=100 right->(right=null result=((guid)) pagesize=100 left=null ) ) (<-left typeguid=9202a8c04000641f80000000000000ca result=((guid typeguid datatype value contents)) optional pagesize=100 right->(right=null result=((guid)) guid=9202a8c04000641f8000000000000092 pagesize=100 left=null ) ) ) (<-left typeguid=9202a8c04000641f800000000000000c result=((guid typeguid contents)) pagesize=100 right->(right=null result=((guid contents)) pagesize=100 left=null (<-left typeguid=9202a8c04000641f80000000000000ca result=((guid typeguid datatype value contents)) optional pagesize=100 right->(right=null result=((guid)) guid=9202a8c04000641f8000000000000092 pagesize=100 left=null ) ) ) ) (<-left typeguid=9202a8c04000641f80000000000000ca result=((guid typeguid datatype value contents)) pagesize=100 datatype=string value~=\"bob*\" right->(right=null result=((guid)) guid=9202a8c04000641f8000000000000092 pagesize=100 left=null ) ) (<-left typeguid=9202a8c04000641f800000000000000a result=((guid typeguid contents)) optional pagesize=100 right->(right=null result=((guid contents)) pagesize=100 left=null (<-left typeguid=9202a8c04000641f80000000000000ca result=((guid typeguid datatype value contents)) optional pagesize=100 right->(right=null result=((guid)) guid=9202a8c04000641f8000000000000092 pagesize=100 left=null ) ) (<-left typeguid=9202a8c04000641f800000000000000c result=((guid typeguid contents)) optional pagesize=100 right->(right=null result=((guid)) pagesize=100 left=null ) ) ) ) (<-left right=null typeguid=9202a8c04000641f80000000011ae4f0 result=((guid value typeguid datatype)) optional pagesize=100 ) (<-right typeguid=9202a8c04000641f8000000000000002 result=((guid typeguid datatype value contents)) optional pagesize=100 left->(right=null result=((guid)) pagesize=100 left=null ) ) (<-left typeguid=9202a8c04000641f800000000000000c result=((guid typeguid contents)) pagesize=1 right->(right=null result=((guid)) guid=9202a8c04000641f80000000011ae4e4 pagesize=100 left=null ) ) )" 215 220) ("127.0.0.1:53578" "status" "RUN" 2007-05-31T00:11:42.0000Z 2007-05-31T00:11:44.0000Z 15 0 1 0 0 "status (conn)" 319 320)))
	EOF
rm -rf $D
