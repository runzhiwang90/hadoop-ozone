#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$DIR/../../.." || exit 1

REPORT_DIR=${OUTPUT_DIR:-"$DIR/../../../target/intermittent"}
mkdir -p "$REPORT_DIR"

export MAVEN_OPTS="-Xmx4096m"
mvn -B install -DskipTests

rc=0
for i in {1..20}; do
  original_report_dir="${REPORT_DIR}"
  REPORT_DIR="${original_report_dir}/iteration${i}"
  mkdir -p "${REPORT_DIR}"

  mvn -B -fae test -DfailIfNoTests=false -Dskip.yarn "$@" \
    | tee "${REPORT_DIR}/output.log"
  irc=$?

  # shellcheck source=hadoop-ozone/dev-support/checks/_mvn_unit_report.sh
  source "${DIR}/_mvn_unit_report.sh"

  REPORT_DIR="${original_report_dir}"
  echo "Iteration ${i} exit code: ${irc}" | tee -a "${REPORT_DIR}/output.log"

  if [[ ${rc} == 0 ]]; then
    rc=${irc}
  fi
done

exit ${rc}
