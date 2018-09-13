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
#
# test.sh -- run test files, check results.
#
# FILES:
#       X.*       -- files related to test case "X".
#       X.sh      -- script that runs case "X".  Executed by test.sh.
#                    Don't use white space or quotes in the name X.
#       X.out     -- stdout from the immediately preceding failed run
#       X.err     -- stderr from the immediately preceding failed run
#       X.out.exp -- expected output.  If missing, no output is expected.
#       X.err.exp -- expected stderr.  If missing, no errors are expected.
#       X.N.gprof -- gprof (profile) data
#

verbose=on
status=0
result=ok

if [ "x$1" == "x-v" ]; then
  verbose=on
  shift
fi

function error() {
  if [ "$result" != "ERROR" ]; then
    result=ERROR
    if [ "$verbose" == "on" ]; then
      echo ERROR
    fi
  fi
  echo "$@"
}

function verbose_log() {
  if [ "$verbose" == "on" ]; then
    echo "$@"
  fi
}

function test_case() {
  if [ "$1" == "test.sh" ]; then
    continue;
  fi

  b="${1%.sh}"
  verbose_log "$b"

  result=ok;
  "$1" > "$b.out" 2> "$b.err"

  if [ -s "$b.err" ]; then
    verbose_log "error file output: "
    verbose_log "$(cat $b.err)"
    verbose_log "---end error output"
    if [ -s "$b.err.exp" ]; then
      egrep -v "(^Stacktrace)|(^$)" $b.err > $b.err.clean
      egrep -v "(^Stacktrace)|(^$)" $b.err.exp > $b.err.exp.clean
      if ! cmp -s "$b.err.clean" "$b.err.exp.clean"; then
        error "$b: changed error output:"
        diff -y $b.err $b.err.exp
      fi
    else
      error "$b: unexpected error output:"
      sed "s^$b: " < "$b.err"
    fi
  else
    if [ -s "$b.err.exp" ]; then
      error "$b: missing error output"
    fi
  fi

  if [ -s "$b.out" ]; then
    verbose_log "output: "
    verbose_log "$(cat $b.out)"
    verbose_log "---end output"
    if [ -s "$b.out.exp" ]; then
      if ! cmp -s "$b.out" "$b.out.exp" ; then
        error "$b: changed output:"
        diff -y $b.out $b.out.exp
      fi
    else
      error "$b: unexpected output:"
      sed "s^$b: " < "$b.out"
    fi
  else
    if [ -s "$b.out.exp" ]; then
      error "$b: missing output"
    fi
  fi

  if [ "$result" == "ok" ]; then
    verbose_log "OK."
  else
    status=1
  fi
}

if [ $# != "0" ]; then
  t=test/unit/$1
  while [ $# != "0" ]; do
    if [ \( -f "$t" \) -a \( -x "$t" \) ]; then
      test_case "$t"
    elif [ -f "${t}sh" -a -x "${t}sh" ]; then
      test_case "${t}sh"
    elif [ -f "$t.sh" -a -x "$t.sh" ]; then
      test_case "$t.sh"
    else
      echo "$0: can't execute $t or ${t}sh or $t.sh"
      status=1
    fi
    shift
  done
else
  for i in ${TEST_SRCDIR}/test/unit/*.sh; do
    if [ "$i" == "test.sh" ]; then
      continue;
    fi

    test_case $i
  done
fi

exit $status
