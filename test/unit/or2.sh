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
write (left->(name="result's left")
	scope->(
		scope->(
			(<-left typeguid->(name="the type")
				right->(name="the right")
			)
		)
		(<-left typeguid->(name="the type")
			right->(name="the right")
		)
	)
)
read (
	left->(name="result's left")
	scope->(
  	    {
		(<-left typeguid->(name="the type")
       	        	 right->(name="the right"))
             	$foo=guid
	    }
           ||
	   {
		  scope->((<-left typeguid->(name="the type")
		     right->(name="the right"))
		  $bar=guid)
	    }
     	 )
       result=(($foo $bar))
)
EOF
rungraphd -d${D} -bty <<-'EOF'
write (left->(name="result's left")
	scope->(
		(<-left typeguid->(name="the type")
			right->(name="the right")
		)
	)
)
read (
	left->(name="result's left")
  	{
     	  scope->(
		(<-left typeguid->(name="the type")
       	        	 right->(name="the right"))
			$foo=guid)
       ||
      	  scope->(
		scope->((<-left typeguid->(name="the type")
		         right->(name="the right"))
			$foo=guid))
       }
       result=$foo
)
EOF
rm -rf $D
rungraphd -d${D} -bty <<-'EOF'
write (left->(name="result's left")
	scope->(
		scope->(
			(<-left typeguid->(name="the type")
				right->(name="the right")
			)
		)
		(<-left typeguid->(name="the type")
			right->(name="the right")
		)
	)
)
read (
	left->(name="result's left")
  	{
     	  scope->(
		(<-left typeguid->(name="the type")
       	        	 right->(name="the right"))
			$foo=guid)
       ||
      	  scope->(
		scope->((<-left typeguid->(name="the type")
		         right->(name="the right"))
			$foo=guid))
       }
       result=$foo
)
read (
	left->(name="result's left")
  	{
      	  scope->(
		scope->((<-left typeguid->(name="the type")
		         right->(name="the right"))
			$foo=guid))
       ||
     	  scope->(
		(<-left typeguid->(name="the type")
       	        	 right->(name="the right"))
			$foo=guid)
       }
       result=$foo
)
EOF
rm -rf $D
