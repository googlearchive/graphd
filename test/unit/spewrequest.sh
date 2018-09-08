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
set -e

B=`basename $0 .sh`
cd `dirname $0`
source ./rungraphd

# Our process name
readonly ME=$(basename "$0")

# Port number used for testing
readonly PORT=9999

# Host used for testing
readonly HOST=localhost
#
# Send commands to our `graphd' server.
#
function do_replay
{
	../multi/replay.pl ${HOST} ${PORT}
}

function do_clean
{
	rm -fr spewrequest.db spewrequest.?.out spewrequest.*.tmp
}

#
# Run `graphd'
#
function do_graphd
{
	rungraphd -d spewrequest.db -f spewrequest.conf "$@"
}

function get_child_pid
{
	# Grab the PID of the worker process and kill SEGV it
	PARENT=`cat spewrequest.pid.tmp`
	# This would be nice but too many versions of ps don't understand
	# --ppid
	#$GRPID=`ps --ppid $PARENT -o pid h`
	# So we do this instead:
	GRPID=`ps -o ppid,pid | grep "^[ ]*$PARENT" | awk '{print $2}'`

#	echo $GRPID child of $PARENT
#	echo $GRPID | grep '^[0-9][0-9]*$' > /dev/null

	if [ $? != "0" ] ; then
		cat <<-END
Tried to figure out the PID of the graphd worker process but I got \"$GRPID\"
which makes no sense as a PID.  Graphd may have crashed or your version of
PS may not understand --ppid
END
		exit 1
	fi
}

# cleanup
do_clean

# start server
#echo "starting up"
do_graphd

sleep 2

get_child_pid

OLD_GRPID=$GRPID

# spew data
#echo "spewing data"
./spewrequest.pl | do_replay

#echo "This should have cut our connection, but not killed the process"

get_child_pid

if [ $GRPID != $OLD_GRPID ] ; then
	echo "Fail -- the process died due to spewing"
	exit 1
fi
 
do_graphd -z
do_clean
