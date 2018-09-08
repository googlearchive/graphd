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

# Walk tentacle ends of a squid. 

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (name="goose"
	 (<- name="duck" (name="duck" (<- name="duck" (name="duck" value="A"))))
	 (<- name="duck" (name="duck" (<- name="duck" (name="duck" value="B"))))
	 (<- name="duck" (name="duck" (<- name="duck" (name="duck" value="C"))))
	 (<- name="duck" (name="duck" (<- name="duck" (name="duck" value="D"))))
	 (<- name="duck" (name="duck" (<- name="duck" (name="duck" value="E"))))
	)
	read (name="duck" (-> name="duck" (name="duck" (-> name="duck" (name="goose"))))
		pagesize="3" result=(cursor (value)))
	read (name="duck" (-> name="duck" (name="duck" (-> name="duck" (name="goose"))))
		cursor="cursor:7a88:fixed:5:2,6,10,14,18/3/"
		pagesize="3" result=(cursor (value)))
	read (name="duck" (-> name="duck" (name="duck" (-> name="duck" (name="goose"))))
		cursor="cursor:7a90:fixed:5:2,6,10,14,18/5/"
		pagesize="3" result=(cursor (value)))
EOF
rm -rf $D
