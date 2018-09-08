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

#  Unique clustering
#
#  The whole unique cluster is a key.  That means that
#  if just parts of a cluster exist, they don't match.
#
#  1   If neither exist, the insertion succeeds.
#
#  2    Mike exists, Anke doesn't.  Write succeeds.
#
#  3	Anke exists, Mike doesn't.  Write succeeds.
#
#  4	Anke and Mike exist, but they're not connected,
#	and the connection is part of the unique constraint.
#	Write succeeds.
#
#  5	Anke and Mike exist, and they're connected to other
#	primitives.
#	The connection is part of the unique constraint.
#	Write succeeds.
#
#  6    Anke and Mike exist and are connected.  The
#	write fails.
#
#  7	Same set-up as case 5.  This time Anke and Mike
# 	each have unique constraints, but their connection
#	isn't part of the unique set.
#	They are two separate clusters, and the 
#	write fails, even though they're used in very
#	different contexts.
#
#  8    Mike with a linkage that's part of the unique cluster,
#	even though the destination isn't.
#	It doesn't conflict with an existing Mike without
#	the connection.
#
#  9    It does conflict with an existing Mike with the
#	connection.  Write fails.
#
#  10	If there is a right= GUID in the unique constraint,
#	the constraint includes the specific destination,
#	not just a general match.   An existing Mike with
#	a different right-hand side can coexist with a new
#	one.
#
rm -rf $D
rungraphd -d${D} -bty <<- 'EOF'
  write (name="1:Mike" unique=(name right) right->(name="1:Anke" unique=name))
  write (name="2:Mike")
  write (name="2:Mike" unique=(name right) right->(name="2:Anke" unique=name))
  write (name="3:Anke")
  write (name="3:Mike" unique=(name right) right->(name="3:Anke" unique=name))
  write (name="4:Mike")
  write (name="4:Anke")
  write (name="4:Mike" unique=(name right) right->(name="4:Anke" unique=name))
  write (name="5:Mike" right->(name="5:Elke"))
  write (name="5:Bob" right->(name="5:Anke"))
  write (name="5:Mike" unique=(name right) right->(name="5:Anke" unique=name))
  write (name="6:Mike" right->(name="6:Anke"))
  write (name="6:Mike" unique=(name right) right->(name="6:Anke" unique=name))
  write (name="7:Mike" right->(name="7:Elke"))
  write (name="7:Bob" right->(name="7:Anke"))
  write (name="7:Mike" unique=(name) right->(name="7:Anke" unique=name))
  write (name="8:Mike")
  write (name="8:Mike" unique=(name right) right->())
  write (name="9:Mike" right->(name="9:Pat"))
  write (name="9:Mike" unique=(name right) right->())
  write (name="10:namespace")
  write (name="10:Mike" right->(name="10:Pat"))
  write (name="10:Mike" right=0000001240003456800000000000001d
  	unique=(name right))
EOF
rm -rf $D
