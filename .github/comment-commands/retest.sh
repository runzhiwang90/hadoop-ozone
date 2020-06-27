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

#doc: provide help on how to trigger new CI build

# posting a new commit from this script does not trigger CI checks
# https://help.github.com/en/actions/reference/events-that-trigger-workflows#triggering-new-workflows-using-a-personal-access-token

set -u
set -x

pr_url="$(jq -r '.issue.pull_request.url' "${GITHUB_EVENT_PATH}")"
commenter="$(jq -r '.comment.user.login' "${GITHUB_EVENT_PATH}")"

curl -LSs "${pr_url}" -o pull.tmp
source_repo="$(jq -r '.head.repo.ssh_url' pull.tmp)"
branch="$(jq -r '.head.ref' pull.tmp)"
pr_owner="$(jq -r '.head.user.login' pull.tmp)"
maintainer_can_modify="$(jq -r '.maintainer_can_modify' pull.tmp)"

if [[ "${commenter}" == "${pr_owner}" ]]; then
  read -r -d '' MESSAGE <<-"EOF"
  To re-run CI checks, please follow these steps with the source branch checked out:
      git commit --allow-empty -m 'trigger new CI check'
      git push
EOF
elif [[ "${maintainer_can_modify}" == "true" ]]; then
  read -r -d '' MESSAGE <<-EOF
  To re-run CI checks, please follow these steps:
      git fetch "${source_repo}" "${branch}"
      git checkout FETCH_HEAD

      git commit --allow-empty -m 'trigger new CI check'
      git push "${source_repo}" HEAD:"${branch}"
EOF
else
  read -r -d '' MESSAGE <<-EOF
@${pr_owner} please trigger new CI check by following these steps:
    git commit --allow-empty -m 'trigger new CI check'
    git push
EOF
fi

echo ">>>${MESSAGE}<<<"
exit

if [[ -z "${MESSAGE}" ]]; then
  exit 1
fi

set +x #GITHUB_TOKEN

curl -s -o /dev/null \
  -X POST \
  --data "$(jq --arg body "${MESSAGE}" -n '{body: $body}')" \
  --header "authorization: Bearer ${GITHUB_TOKEN}" \
  --header 'content-type: application/json' \
  "$(jq -r '.issue.comments_url' "${GITHUB_EVENT_PATH}")"
