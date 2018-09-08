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
rungraphd -d${D}  -bty <<-'EOF'
	write (type="camera"
		(-> type="name" value="Canon PowerShot A430")
		(-> type="sensorpixel" value="4.1")
	      )
	write (type="camera"
		(-> type="name" value="Canon PowerShot A510")
		(-> type="sensorpixel" value="3.3")
	      )
	write (type="camera"
		(-> type="name" value="Canon PowerShot A520")
		(-> type="sensorpixel" value="3.9")
	      )
	write (type="camera"
		(-> type="name" value="Canon PowerShot A530")
		(-> type="sensorpixel" value="5.3")
	      )
	write (type="camera"
		(-> type="name" value="Canon PowerShot A540")
		(-> type="sensorpixel" value="6.2")
	      )
	write (type="camera"
		(-> type="name" value="Canon PowerShot A610")
		(-> type="sensorpixel" value="5.0")
	      )
	write (type="camera"
		(-> type="name" value="Canon PowerShot A620")
		(-> type="sensorpixel" value="7.1")
	      )
	read (type="camera" result=(($name)) (-> type="sensorpixel" value >= "6") (-> type="name" $name=value))
	EOF
rm -rf $D
