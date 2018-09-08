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

# start up a graph
rungraphd -i tcp::8102 -d${D} -p ${D}.pid

# run the gpush tool against it.
# It'll send up to 100,000 (-n 100000) "write ()\n" requests, and
# terminate successfully only if the socket to graphd didn't become
# readable for 10 (-w 10) seconds.
if ../../graphutil/${OBJ}/gpush -s tcp::8102 -n 100000 -w 10 -z 
then
	echo gpush: ok
else
	echo gpush: failed 1>&2
fi

# one that's done, shut down graph again.
rungraphd -i tcp::8102 -d${D} -p ${D}.pid -z
rm -rf $D
