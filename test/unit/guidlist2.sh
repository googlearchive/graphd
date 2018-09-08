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

# these aren't actually guidlists, but templates for guidlist3/4.

rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write (name="a")
	write (name="b")
	write (name="c")
	write (name=null)
	read id="+++ name +++" (any result=((name)))
	read id="name = = b"        (result=((name)) name=("a" "b")   name=("b" "c"))
	read id="name = = EMPTY"    (result=((name)) name=("a" "b")   name=("c"))
	read id="name = = null"     (result=((name)) name=("a" null)  name=(null "b"))
	read id="name = = null"     (result=((name)) name=("a" null)  name=null)
	read id="name = != b"       (result=((name)) name=("a" "b")   name!=("a"))
	read id="name = != EMPTY"   (result=((name)) name=("a" "b")   name!=("a" "b"))
	read id="name = != null"    (result=((name)) name=("a" null)  name!=("a"))
	read id="name = != a"       (result=((name)) name=("a" null)  name!=null)
	read id="name != = c"       (result=((name)) name!=("a" "b")  name=("b" "c"))
	read id="name != = EMPTY"   (result=((name)) name!=("a" "b")  name=("a"))
	read id="name != = b"       (result=((name)) name!=("a" null) name=(null "b"))
	read id="name != = abc"     (result=((name)) name!=(null)     name=("b" "c" "a"))
	read id="name != != c"      (result=((name)) name!=("a" "b")  name!=("b" null))
	read id="name != != EMPTY"  (result=((name)) name!=("a" "b")  name!=(null "c"))
	read id="name != != b"      (result=((name)) name!=("a" null) name!=("c"))
	read id="name != != abc"    (result=((name)) name!=(null)     name!=null)
	restore ("5" 0 0)
	write (value="a")
	write (value="b")
	write (value="c")
	write (value=null)
	read id="value = = b"       (result=((value)) value=("a" "b")   value=("b" "c"))
	read id="value = = EMPTY"   (result=((value)) value=("a" "b")   value=("c"))
	read id="value = = null"    (result=((value)) value=("a" null)  value=(null "b"))
	read id="value = = null"    (result=((value)) value=("a" null)  value=null)
	read id="value = != b"      (result=((value)) value=("a" "b")   value!=("a"))
	read id="value = != EMPTY"  (result=((value)) value=("a" "b")   value!=("a" "b"))
	read id="value = != null"   (result=((value)) value=("a" null)  value!=("a"))
	read id="value = != a"      (result=((value)) value=("a" null)  value!=null)
	read id="value = ~= b"      (result=((value)) value=("a" "b")   value~=("b"))
	read id="value = ~= EMPTY"  (result=((value)) value=("a" "b")   value~=("c"))
	read id="value = ~= null"   (result=((value)) value=("a" null)  value~=())
	read id="value = ~= null"   (result=((value)) value=("a" null)  value~=null)
	read id="value = ~= null"   (result=((value)) value=("a" null)  value~=(null))
	read id="value = ~= a"      (result=((value)) value=("a" null)  value~=("a" "b" "c"))
	read id="value != = c"      (result=((value)) value!=("a" "b")  value=("b" "c"))
	read id="value != = EMPTY"  (result=((value)) value!=("a" "b")  value=("a"))
	read id="value != = b"      (result=((value)) value!=("a" null) value=(null "b"))
	read id="value != = abc"    (result=((value)) value!=(null)     value=("b" "c" "a"))
	read id="value != != c"     (result=((value)) value!=("a" "b")  value!=("b" null))
	read id="value != != EMPTY" (result=((value)) value!=("a" "b")  value!=(null "c"))
	read id="value != != b"     (result=((value)) value!=("a" null) value!=("c"))
	read id="value != != abc"   (result=((value)) value!=(null)     value!=null)
	read id="value != ~= c"     (result=((value)) value!=("a" "b")  value~=("c"))
	read id="value != ~= EMPTY" (result=((value)) value!=("a" "b")  value~=("a"))
	read id="value != ~= b"     (result=((value)) value!=("a" null) value~=("b"))
	read id="value != ~= abc"   (result=((value)) value!=(null)     value~=("a" "b" "c"))
	read id="value ~= = b"      (result=((value)) value=("a" "b")   value=("b" "c"))
	read id="value ~= = EMPTY"  (result=((value)) value=("a" "b")   value=("c"))
	read id="value ~= = null"   (result=((value)) value=("a" null)  value=(null "b"))
	read id="value ~= = null"   (result=((value)) value=("a" null)  value=null)
	read id="value ~= != b"     (result=((value)) value=("a" "b")   value!=("a"))
	read id="value ~= != EMPTY" (result=((value)) value=("a" "b")   value!=("a" "b"))
	read id="value ~= != null"  (result=((value)) value=("a" null)  value!=("a"))
	read id="value ~= != a"     (result=((value)) value=("a" null)  value!=null)
	read id="value ~= ~= b"     (result=((value)) value=("a" "b")   value~=("b"))
	read id="value ~= ~= EMPTY" (result=((value)) value=("a" "b")   value~=("c"))
	read id="value ~= ~= null"  (result=((value)) value=("a" null)  value~=())
	read id="value ~= ~= null"  (result=((value)) value=("a" null)  value~=null)
	read id="value ~= ~= null"  (result=((value)) value=("a" null)  value~=(null))
	read id="value ~= ~= a"     (result=((value)) value=("a" null)  value~=("a" "b" "c"))
	EOF
rm -rf $D
