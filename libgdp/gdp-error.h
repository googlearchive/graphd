/*
Copyright 2015 Google Inc. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#ifndef GDP_ERROR_H
#define GDP_ERROR_H

/* Error codes
 */
#define GDP_ERR_BASE (-1000)
#define GDP_ERR_LEXICAL (GDP_ERR_BASE + 0)
#define GDP_ERR_SYNTAX (GDP_ERR_BASE + 1)
#define GDP_ERR_SEMANTICS (GDP_ERR_BASE + 2)
#define GDP_ERR_MALFORMED (GDP_ERR_BASE + 3)
#define GDP_ERR_TOO_LONG (GDP_ERR_BASE + 4)
#define GDP_ERR_AGAIN (GDP_ERR_BASE + 5)

#endif /* GDP_ERROR_H */
