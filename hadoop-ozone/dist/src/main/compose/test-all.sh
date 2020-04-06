#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#
# Test executor to test all the compose/*/test.sh test scripts.
#

SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )
ALL_RESULT_DIR="$SCRIPT_DIR/result"

t="$SCRIPT_DIR"/ozone-topology/test.sh
d="$(dirname "$t")"
r="${d}/result"
echo "Executing test in ${d}"
#required to read the .env file from the right location
cd "${d}"

df -h

for i in {1..10}; do
  echo "Iteration ${i}"

  dd if=/dev/zero of="hog${i}" bs=1024 count=1048576 conv=fsync
  df -h

  ./test.sh
  ret=$?
  if [[ ${ret} -ne 0 ]]; then
    cp "${r}"/robot-*.xml "${r}"/docker-*.log "${r}"/*.out* "$ALL_RESULT_DIR"/
    rebot -N "smoketests" -d "${r}" "${r}/robot-*.xml"
    exit ${ret}
  fi
done
