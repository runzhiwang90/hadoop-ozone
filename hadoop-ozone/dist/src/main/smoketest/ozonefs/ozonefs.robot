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

*** Settings ***
Documentation       Ozonefs test covering both o3fs and ofs
Library             OperatingSystem
Resource            ../commonlib.robot
Resource            setup.robot
Test Timeout        5 minutes
Suite Setup         Setup ${BUCKET_TYPE}s for FS test

*** Variables ***
${scheme}     ofs

*** Test Cases ***
Test single FS operations
    [Template]    Test ${scheme} FS
    ofs
    o3fs

Test cross-FS operations
    [Template]    Test cross-FS operations from ${scheme} to ${scheme2}
    ${scheme}    ofs
    ${scheme}    o3fs

List root
    [Template]    List root
    ofs
    o3fs

List non-existent volume
    [Template]    List non-existent volume
    ofs
    o3fs

List non-existent bucket
    [Template]    List non-existent bucket
    ofs
    o3fs


*** Keywords ***
Test ${scheme} FS
    ${base_url} =       Format FS URL    ${scheme}     ${VOLUME}    ${BUCKET}    /
    ${deep_dir} =       Set Variable      test/${scheme}/dir
    ${deep_url} =       Set Variable      ${base_url}${deep_dir}

                        Execute           ozone fs -mkdir -p ${deep_url}
    ${result} =         Execute           ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                        Should contain    ${result}         ${deep_dir}

                        Execute           ozone fs -copyFromLocal NOTICE.txt ${deep_url}/
    ${result} =         Execute           ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                        Should contain    ${result}         NOTICE.txt
    ${result} =         Execute           ozone sh key info ${VOLUME}/${BUCKET}/${deep_dir}/NOTICE.txt | jq -r '.replicationFactor'
                        Should Be Equal   ${result}         3

                        Execute               ozone fs -put NOTICE.txt ${deep_url}/PUTFILE.txt
    ${result} =         Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                        Should contain    ${result}         PUTFILE.txt

    ${result} =         Execute               ozone fs -ls ${deep_url}/
                        Should contain    ${result}         NOTICE.txt
                        Should contain    ${result}         PUTFILE.txt

                        Execute               ozone fs -mv ${deep_url}/NOTICE.txt ${deep_url}/MOVED.TXT
    ${result} =         Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                        Should contain    ${result}         MOVED.TXT
                        Should not contain  ${result}       NOTICE.txt

                        Execute               ozone fs -mkdir -p ${deep_url}/subdir1
                        Execute               ozone fs -cp ${deep_url}/MOVED.TXT ${deep_url}/subdir1/NOTICE.txt
    ${result} =         Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                        Should contain    ${result}         subdir1/NOTICE.txt

    ${result} =         Execute               ozone fs -ls ${deep_url}/subdir1/
                        Should contain    ${result}         NOTICE.txt

                        Execute               ozone fs -cat ${deep_url}/subdir1/NOTICE.txt
                        Should not contain  ${result}       Failed

                        Execute               ozone fs -rm ${deep_url}/subdir1/NOTICE.txt
    ${result} =         Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                        Should not contain  ${result}       NOTICE.txt

    ${result} =         Execute               ozone fs -rmdir ${deep_url}/subdir1/
    ${result} =         Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                        Should not contain  ${result}       subdir1

                        Execute               ozone fs -touch ${base_url}${deep_dir}/TOUCHFILE-${scheme}.txt
    ${result} =         Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                        Should contain  ${result}       TOUCHFILE-${scheme}.txt

                        Execute               ozone fs -rm -r ${base_url}${deep_dir}/
    ${result} =         Execute               ozone sh key list ${VOLUME}/${BUCKET} | jq -r '.name'
                        Should not contain  ${result}       ${deep_dir}

                        Execute               rm -Rf /tmp/localdir1
                        Execute               mkdir /tmp/localdir1
                        Execute               cp NOTICE.txt /tmp/localdir1/LOCAL.txt
                        Execute               ozone fs -mkdir -p ${base_url}testdir1
                        Execute               ozone fs -copyFromLocal /tmp/localdir1 ${base_url}testdir1/
                        Execute               ozone fs -put NOTICE.txt ${base_url}testdir1/NOTICE.txt

    ${result} =         Execute               ozone fs -ls -R ${base_url}testdir1/
                        Should contain    ${result}         localdir1/LOCAL.txt
                        Should contain    ${result}         testdir1/NOTICE.txt

                        Execute               ozone sh key put ${VOLUME}/${BUCKET}/${scheme}.txt NOTICE.txt
    ${result} =         Execute               ozone fs -ls ${base_url}${scheme}.txt
                        Should contain    ${result}         ${scheme}.txt

    ${rc}  ${result} =  Run And Return Rc And Output        ozone fs -copyFromLocal NOTICE.txt ${base_url}${scheme}.txt
                        Should Be Equal As Integers     ${rc}                1
                        Should contain    ${result}         File exists

                        Execute               rm -Rf /tmp/GET.txt
                        Execute               ozone fs -get ${base_url}${scheme}.txt /tmp/GET.txt
                        Execute               ls -l /tmp/GET.txt

Test cross-FS operations from ${scheme1} to ${scheme2}
    Cross-FS copy    ${scheme1}    ${VOLUME}    ${BUCKET}    ${scheme2}    ${VOLUME}    ${BUCKET2}
    Cross-FS copy    ${scheme1}    ${VOLUME}    ${BUCKET}    ${scheme2}    ${VOL2}      ${BUCKET_IN_VOL2}


Cross-FS copy
    [arguments]    ${scheme1}    ${vol1}    ${bucket1}    ${scheme2}    ${vol2}    ${bucket2}

    ${base1} =     Format FS URL    ${scheme1}    ${vol1}    ${bucket1}   /
    ${base2} =     Format FS URL    ${scheme2}    ${vol2}    ${bucket2}   /
    ${target} =    Set Variable    ${base2}/${scheme1}-${scheme2}
    Execute        ozone fs -mkdir ${target}
    Execute        ozone fs -cp ${base1}testdir1/localdir1 ${target}/

List root
    [arguments]    ${scheme}

    ${root} =           Format FS URL    ${scheme}     ${VOLUME}    ${BUCKET}
                        Execute           ozone fs -ls ${root}


List non-existent volume
    [arguments]    ${scheme}

    ${url} =       Format FS URL          ${scheme}    no-such-volume    ${BUCKET}
    ${result} =    Execute and checkrc    ozone fs -ls ${url}     1
                   Should Match Regexp    ${result}         (Check access operation failed)|(Volume no-such-volume not found)|(No such file or directory)


List non-existent bucket
    [arguments]    ${scheme}

    ${url} =       Format FS URL          ${scheme}    ${VOLUME}    no-such-bucket
    ${result} =    Execute and checkrc    ozone fs -ls ${url}     1
                   Should Match Regexp    ${result}         (Check access operation failed)|(Bucket not found)|(No such file or directory)

