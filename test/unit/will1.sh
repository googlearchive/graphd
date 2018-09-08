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

rm -rf $D $B.pid
#	$ts > 2005-05-06T02:20:34.0009,

rungraphd -d${D} -bty -pwill3.pid -by << 'EOF'
write ("metaweb.0.1.clob" 
	(-> "metaweb.0.1.clob.url"
		("metaweb.0.1.url" value="clob:12345"))
	(-> "metaweb.0.1.clob.mimetype"
		("metaweb.0.1.mimetime" value="text/haiku")))
write ("metaweb.0.1.clob" 
	(-> "metaweb.0.1.clob.url"
		("metaweb.0.1.url" value="clob:34567"))
	(-> "metaweb.0.1.clob.mimetype"
		("metaweb.0.1.mimetime" value="food/waffle")))
write ("metaweb.0.1.clob" 
	(-> "metaweb.0.1.clob.url"
		("metaweb.0.1.url" value="clob:56789"))
	(-> "metaweb.0.1.clob.mimetype"
		("metaweb.0.1.mimetime" value="application/silly-string")))
read ("metaweb.0.1.clob"
		result=(($mimetype $clobid $ts))
		newest=0
		sort=($ts)
		(-> "metaweb.0.1.clob.url"
			("metaweb.0.1.url" $clobid=value $ts=timestamp))
		(-> "metaweb.0.1.clob.mime-type"
			($mimetype=value
			timestamp < 2005-05-06T02:20:34.0014)))
EOF
rm -rf $D $B.pid
