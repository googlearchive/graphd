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
	write (value="apples")
	write (value="oranges")
	read (sort=(count) result=())
	read (sort=(archival) result=())
	read (sort=($foo) result=())
	read (sort=(datatype) result=())
	read (sort=(generation) result=())
	read (sort=(guid) result=())
	read (sort=(right) result=())
	read (sort=(left) result=())
	read (sort=(scope) result=())
	read (sort=(typeguid) result=())
	read (sort=(literal="foo") result=())
	read (sort=(live) result=())
	read (sort=(meta) result=())
	read (sort=(name) result=())
	read (sort=(next) result=())
	read (sort=(previous) result=())
	read (sort=(timestamp) result=())
	read (sort=(type) result=())
	read (sort=(value) result=())
	read (sort=(()) result=())
	read (sort=(cursor) result=())
	read (sort=(estimate) result=())
	read (sort=(valuetype) result=())
	read (sort=(iterator) result=())
	read (sort=(timeout) result=())
	read (sort=(estimate-count) result=())

	read (sort=count result=())
	read (sort=archival result=())
	read (sort=$foo result=())
	read (sort=datatype result=())
	read (sort=generation result=())
	read (sort=guid result=())
	read (sort=right result=())
	read (sort=left result=())
	read (sort=scope result=())
	read (sort=typeguid result=())
	read (sort=literal="foo" result=())
	read (sort=live result=())
	read (sort=meta result=())
	read (sort=name result=())
	read (sort=next result=())
	read (sort=previous result=())
	read (sort=timestamp result=())
	read (sort=type result=())
	read (sort=value result=())
	read (sort=() result=())
	read (sort=cursor result=())
	read (sort=estimate result=())
	read (sort=valuetype result=())
	read (sort=iterator result=())
	read (sort=timeout result=())
	read (sort=estimate-count result=())

	read (sort=($foo) $foo=archival result=())
	read (sort=($foo) $foo=count result=())
	read (sort=($foo) $foo=$bar result=())
	read (sort=($foo) $foo=datatype result=())
	read (sort=($foo) $foo=generation result=())
	read (sort=($foo) $foo=guid result=())
	read (sort=($foo) $foo=right result=())
	read (sort=($foo) $foo=left result=())
	read (sort=($foo) $foo=scope result=())
	read (sort=($foo) $foo=typeguid result=())
	read (sort=($foo) $foo=literal="foo" result=())
	read (sort=($foo) $foo=live result=())
	read (sort=($foo) $foo=meta result=())
	read (sort=($foo) $foo=name result=())
	read (sort=($foo) $foo=next result=())
	read (sort=($foo) $foo=previous result=())
	read (sort=($foo) $foo=timestamp result=())
	read (sort=($foo) $foo=type result=())
	read (sort=($foo) $foo=value result=())
	read (sort=($foo) $foo=() result=())
	read (sort=($foo) $foo=cursor result=())
	read (sort=($foo) $foo=estimate result=())
	read (sort=($foo) $foo=valuetype result=())
	read (sort=($foo) $foo=iterator result=())
	read (sort=($foo) $foo=timeout result=())
	read (sort=($foo) $foo=estimate-count result=())
EOF
rm -rf $D
