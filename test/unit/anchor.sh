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
	write id="empty" (typeguid->(value="foo" anchor))
	write id="empty(2)" (anchor typeguid->(value="foo"))
	write id="ok" (typeguid->(value="bar"))
	write id="ok" (typeguid->(value="bar" anchor))
	write id="ok" (typeguid->(value="bar" anchor))
	write id="toomany" (anchor typeguid->(value="bar"))
	write id="ok" (anchor value="bar" (<-typeguid))
	write id="ok" ((<-left value="bob")) 
	write id="ok" (value="frankie")
	write id="ok" (value="friend")
	write id="ok" ((<-left value="bob" anchor)
		(<-left typeguid->(anchor value="friend") right->(anchor value="frankie"))
		(<-left typeguid->(anchor value="friend") right->(value="johnnie"))
	)
	read id="syntax" (key=())
	read id="syntax" (anchor)
	EOF
rm -rf $D
