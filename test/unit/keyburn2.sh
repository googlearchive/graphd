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

#  Illustrating the examples on 
#  https://zwiki.metaweb.com/am/GraphdWriteQuery
#
#  1	first picture (upper left corner).  A keyed constraint "Mike" points to
#	an unkeyed constraint "Anke" with a keyed link from
# 	keyed to unkeyed.  (Let's use "RIGHT")
#
#  1.1 If the right side doesn't exist at all,
#	both sides are created.
#
#  1.2 If the right side exists, but doesn't have the desired
#	values, the right side will be created, and the left
#	will be versioned.
#
#	-- first create a left-right pair in which the  
#	   right side is wrong -- "Elke", not "Anke".
#
#	-- then rewrite Mike with the correct right side.  
#	   Mike must be versioned, not completely rewritten.
#
#	-- read (any newest=1 result=((name))) should print
#	   only ((1.2:Mike))
#
#  1.3 If both sides exist exactly as desired, nothing 
#	is created.
#
#  1.4 If the right side exists as desired, but the left
#	side doesn't exist, both sides are newly created.
#	(The new left doesn't somehow pick up the old right.)
#
rm -rf $D
echo 'Round 1'
rungraphd -d${D} -bty <<- 'EOF'
	write (name="1.1:Mike")
	write (name="1.1:Mike" key=(name right) right->(name="1.1:Anke"))
	write (name="1.2:Mike" right->(name="1.2:Elke"))
	write (name="1.2:Mike" key=(name right) right->(name="1.2:Anke"))
	read (any newest=1 result=((name)))
	write (name="1.3:Mike" key=(name right) right->(name="1.3:Anke"))
	write (name="1.3:Mike" key=(name right) right->(name="1.3:Anke"))
	write (name="1.4:Bob" key=(name right) right->(name="1.4:Anke"))
	write (name="1.4:Mike" key=(name right) right->(name="1.4:Anke"))
	read (any newest=1 result=((name)))
	EOF

#
#  2	second picture (upper right conrer).  Almost like before, but this time
#	the pointer between Mike and Anke is not itself part
#	of Mike's key constraints.
#
#  2.1 If the right side doesn't exist at all,
#	both sides are created.
#
#  2.2 If the right side exists, but doesn't have the desired
#	values, the right side will be created, and the left
#	will be versioned.
#
#	-- first create a left-right pair in which the  
#	   right side is wrong -- "Elke", not "Anke".
#
#	-- then rewrite Mike with the correct right side.  
#	   Mike must be versioned, not completely rewritten.
#
#	-- read (any newest=1 result=((name))) will print
#	   both (("2.1:Mike") (2.2:Mike))
#	  (This is a difference to 1.2.)
#
#  2.3 If both sides exist exactly as desired, nothing 
#	is created.
#
#  2.4 If the right side exists as desired, but the left
#	side doesn't exist, both sides are newly created.
#	(The new left doesn't somehow pick up the old right.)
#
echo 'Round 2'
rungraphd -d${D}  -bty <<- 'EOF'
	write (name="2.1:Mike")
	write (name="2.1:Mike" key=(name) right->(name="2.1:Anke"))
	write (name="2.2:Mike" right->(name="2.2:Elke"))
	write (name="2.2:Mike" key=(name) right->(name="2.2:Anke"))
	read (any newest=1 result=((name)))
	write (name="2.3:Mike" key=(name) right->(name="2.3:Anke"))
	write (name="2.3:Mike" key=(name) right->(name="2.3:Anke"))
	write (name="2.4:Bob" key=(name) right->(name="2.4:Anke"))
	write (name="2.4:Mike" key=(name) right->(name="2.4:Anke"))
	read (any newest=1 result=((name)))
	EOF

#
#  3	third picture.   A keyed constraint with an unkeyed
#	constraint pointing to it.
#
#	The unkeyed constraint is always instantiated -- even if they
#	both match exactly.
#
#  3.1  If nothing exists, both sides are created.
#
#  3.2 	If both sides exist, the right (unkeyed) side is
#	still created.
#	(The read shows that there are two 3.2:Anke.)
#
#  3.3 	If only the left exists, the right (unkeyed) side
#	is created.
#	(The read shows there is one 3.3:Mike in all of space and time.)
#
#  3.4  If only the right exists, both sides are created.
#	(The read shows there are two 3.4:Anke.)
#
#  3.5  If both sides exist, but the left is different,
#	both sides are still created.
#	(The read shows there are two 3.5:Anke.)
#
echo 'Round 3'
rungraphd -d${D}  -bty <<- 'EOF'
	write (name="3.1:Mike" key=(name) (<-right name="3.1:Anke"))
	write (name="3.2:Mike" key=(name) (<-right name="3.2:Anke"))
	write (name="3.2:Mike" key=(name) (<-right name="3.2:Anke"))
	read (any name="3.2:Anke" result=count)
	write (name="3.3:Mike" key=(name))
	write (name="3.3:Mike" key=(name) (<-right name="3.3:Anke"))
	read (any name="3.3:Mike" newest>=0 result=count)
	write (name="3.4:Anke")
	write (name="3.4:Mike" key=(name) (<-right name="3.4:Anke"))
	read (any name="3.4:Anke" result=count)
	write (name="3.5:Bob" (<-right name="3.5:Anke"))
	write (name="3.5:Mike" (<-right name="3.5:Anke"))
	read (any name="3.4:Anke" result=count)
	EOF
rm -rf $D

#
#  4	fourth picture (lower left).   Two keyed constraints 
#	with an unkeyed connection.
#
#
#  4.1  If nothing exists, both sides are created.
#
#  4.2 	If both sides exist with their connection in place,
#	nothing is versioned. Both reads show just 1 record. 
#
#  4.3 	If both sides exist without the connection,
#	the pointer source (Mike) is versioned.  The 
#	reads show two 4.3:Mike, but only one 4.3:Suki
#
#  4.4 	If both sides exist without the connection,
#	but the left points somewhere else, the results
#	are still the same.  The pointer source (Mike)
#	is versioned.  The reads show two 4.4:Mike,
#	but only one 4.4:Suki
#
#  4.5  If only the right side (Suki) exists, the left
#	is created to point to it.
#	The read shows one version of the right side.
#
#  4.6  If only the left side (Mike) exists, the right
#	is created, the left is versioned to point to it.
#	The reads show two versions of the left,
#	but only one version of the right.
#
echo 'Round 4'
rungraphd -d${D}  -bty <<- 'EOF'
	write (name="4.1:Mike" key=name right->(name="4.1:Suki" key=name))
	write (name="4.2:Mike" right->(name="4.2:Suki"))
	write (name="4.2:Mike" key=name right->(name="4.2:Suki" key=name))
	read (name="4.2:Mike" newest>=0 result=(name count))
	read (name="4.2:Suki" newest>=0 result=(name count))
	write (name="4.3:Mike")
	write (name="4.3:Suki")
	write (name="4.3:Mike" key=name right->(name="4.3:Suki" key=name))
	read (name="4.3:Mike" newest>=0 result=(name count))
	read (name="4.3:Suki" newest>=0 result=(name count))
	write (name="4.4:Mike" right->(name="4.4:Anke"))
	write (name="4.4:Suki")
	write (name="4.4:Mike" key=name right->(name="4.4:Suki" key=name))
	read (name="4.4:Mike" newest>=0 result=(name count))
	read (name="4.4:Suki" newest>=0 result=(name count))
	write (name="4.5:Suki")
	write (name="4.5:Mike" key=name right->(name="4.5:Suki" key=name))
	read (name="4.5:Mike" newest>=0 result=(name count))
	read (name="4.5:Suki" newest>=0 result=(name count))
	write (name="4.6:Mike")
	write (name="4.6:Mike" key=name right->(name="4.6:Suki" key=name))
	read (name="4.6:Mike" newest>=0 result=(name count))
	read (name="4.6:Suki" newest>=0 result=(name count))
	EOF
rm -rf $D
#
#  5	fifth picture (lower right).   Two keyed constraints,
#	with a keyed connection.
#
#  5.1  If nothing exists, both sides are created.
#
#  5.2 	If both sides exist with their connection in place,
#	nothing is versioned. Both reads show just 1 record. 
#
#  5.3 	If both sides exist without the connection,
#	a completely new pair is created.
#	The reads show two 5.3:Mike, two 5.3:Suki.
#
#  5.4 	If both sides exist without the connection,
#	but the left points somewhere else, the results
#	are still the same.
#	A completely new pair is created.
#	The reads show two 5.4:Mike, two 5.4:Suki.
#
#  5.5  If only the right side (Suki) exists,
#	a completely new pair is created.
#	The reads show one 5.5:Mike, two 5.5:Suki.
#
#  5.6  If only the left side (Mike) exists,
#	a completely new pair is created.
#	The reads show two 5.6:Mike, one 5.6:Suki.
#
echo 'Round 5'
rungraphd -d${D}  -bty <<- 'EOF'
      write (name="5.1:Mike" key=(name right) right->(name="5.1:Suki" key=name))
      write (name="5.2:Mike" right->(name="5.2:Suki"))
      write (name="5.2:Mike" key=(name right) right->(name="5.2:Suki" key=name))
      read (name="5.2:Mike" newest>=0 result=(name count))
      read (name="5.2:Suki" newest>=0 result=(name count))
      write (name="5.3:Mike")
      write (name="5.3:Suki")
      write (name="5.3:Mike" key=(name right) right->(name="5.3:Suki" key=name))
      read (name="5.3:Mike" newest>=0 result=(name count))
      read (name="5.3:Suki" newest>=0 result=(name count))
      write (name="5.4:Mike" right->(name="5.4:Anke"))
      write (name="5.4:Suki")
      write (name="5.4:Mike" key=(name right) right->(name="5.4:Suki" key=name))
      read (name="5.4:Mike" newest>=0 result=(name count))
      read (name="5.4:Suki" newest>=0 result=(name count))
      write (name="5.5:Suki")
      write (name="5.5:Mike" key=(name right) right->(name="5.5:Suki" key=name))
      read (name="5.5:Mike" newest>=0 result=(name count))
      read (name="5.5:Suki" newest>=0 result=(name count))
      write (name="5.6:Mike")
      write (name="5.6:Mike" key=(name right) right->(name="5.6:Suki" key=name))
      read (name="5.6:Mike" newest>=0 result=(name count))
      read (name="5.6:Suki" newest>=0 result=(name count))
	EOF
rm -rf $D
