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
write ("company" (<- "is-leader-of" name="Leadership Title" value="Chairman & CEO" ("person" (<- "is-attribute-of" ("attribute" name="full name" value="William Clay Ford, Jr.")))) (<- "is-attribute-of" ("attribute" name="Employee Count" value="327531" datatype=integer)) (<- "is-attribute-of" ("collection" value="Product Brands" (<- "is-in-collection" ("name" name="brand name" value="Ford")) (<- "is-in-collection" ("name" name="brand name" value="Lincoln")) (<- "is-in-collection" ("name" name="brand name" value="Mercury")) (<- "is-in-collection" ("name" name="brand name" value="Mazda")) (<- "is-in-collection" ("name" name="brand name" value="Volvo")) (<- "is-in-collection" ("name" name="brand name" value="Jaguar")) (<- "is-in-collection" ("name" name="brand name" value="Land Rover")) (<- "is-in-collection" ("name" name="brand name" value="Aston Martin")))) (<- "is-attribute-of" ("attribute" name="Homepage URL" value="http://www.ford.com/" datatype=url)))
EOF
rm -rf $D
