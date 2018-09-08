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
	write ("concept" value="c1" (<- "is-edit-of" ("wikiversion" value="A" (<- "is-author-of" ("person" (-> "attribute" name="fullname" value="Afirst Alast"))))) (<- "is-edit-of" ("wikiversion" value="B" (<- "is-author-of" ("person" (-> "attribute" name="fullname" value="Bfirst Blast"))))) (<- "is-edit-of" ("wikiversion" value="C" (<- "is-author-of" ("person" (-> "attribute" name="fullname" value="Cfirst Clast"))))))
	read (value="c1" result=contents (<- "is-edit-of" result=((contents)) ("wikiversion" result=(value contents) (<- "is-author-of" result=contents ("person" result=contents (-> "attribute" name="fullname" result=value))))))
EOF
rm -rf $D
