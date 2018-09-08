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

# test the new "!=" operator
rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
	write ()
	write (name="a")
	write (name=("b"))
	write (name=("a" "b"))
	write (name=(null "b"))
	read (result=((name)) name=())
	read (result=((name)) name=null)
	read (result=((name)) name=(null))
	read (result=((name)) name=(null null null))
	read (result=((name)) name=("a"))
	read (result=((name)) name=("a" "a"))
	read (result=((name)) name=("a" "b"))
	read (result=((name)) name=("a" "b" "a"))
	read (result=((name)) name=(null "a"))
	read (result=((name)) name=("a" "a" null))
	read (result=((name)) name=("a" "a" null) name=("b"))
	read (result=((name)) name=("a" "a" null) name=("b" "a"))
	read (result=((name)) name=(null) name=null)
	read (result=((name)) name=("a" "b" null) name=("b" "a"))
	restore ("1" 0 0)
	write ()
	write (value="a")
	write (value=("b"))
	write (value=("a" "b"))
	write (value=(null "b"))
	read (result=((value)) value=())
	read (result=((value)) value=null)
	read (result=((value)) value=(null))
	read (result=((value)) value=(null null null))
	read (result=((value)) value=("a"))
	read (result=((value)) value=("a" "a"))
	read (result=((value)) value=("a" "b"))
	read (result=((value)) value=("a" "b" "a"))
	read (result=((value)) value=(null "a"))
	read (result=((value)) value=("a" "a" null))
	read (result=((value)) value=("a" "a" null) value=("b"))
	read (result=((value)) value=("a" "a" null) value=("b" "a"))
	read (result=((value)) value=(null) value=null)
	read (result=((value)) value=("a" "b" null) value=("b" "a"))
	restore ("1" 0 0)
	write id="write null" ()
	write id="write a" (type="a")
	write id="write b" (type=("b"))
	write id="contradictory" (type=("a" "b"))
	write id="contradictory" (type=(null "b"))
	read id="7xnull" (result=((type)) type=())
	read id="7xnull" (result=((type)) type=null)
	read id="7xnull" (result=((type)) type=(null))
	read id="7xnull" (result=((type)) type=(null null null))
	read id="a" (result=((type)) type=("a"))
	read id="a" (result=((type)) type=("a" "a"))
	read id="a b" (result=((type)) type=("a" "b"))
	read id="a b" (result=((type)) type=("a" "b" "a"))
	read id="null a" (result=((type)) type=(null "a"))
	read id="null a" (result=((type)) type=("a" "a" null))
	read id="empty" (result=((type)) type=("a" "a" null) type=("b"))
	read id="a" (result=((type)) type=("a" "a" null) type=("b" "a"))
	read id="7xnull" (result=((type)) type=(null) type=null)
	read id="a b" (result=((type)) type=("a" "b" null) type=("b" "a"))
	EOF
rm -rf $D

