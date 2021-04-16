#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# set -e
# set -o pipefail
# set -o errexit

# prints summaries of failed tests to console
# by using the targer/surefire-reports files
# works only when testForkCount > 1 since that is when surefire will create reports for individual test classes
function print_testng_failures() {
  (
    { set +x; } 2>/dev/null
    local testng_failed_file="$1"
    local report_prefix="${2:-Test failure in}"
    local group_title="${3:-Detailed test failures}"
    if [ -f "$testng_failed_file" ]; then
      local testng_report_dir=$(dirname "$testng_failed_file")
      local failed_count=0
      for failed_test_class in $(cat "$testng_failed_file" | grep 'class name=' | perl -p -e 's/.*\"(.*?)\".*/$1/'); do
        ((failed_count += 1))
        if [ $failed_count -eq 1 ]; then
          echo "::endgroup::"
          echo "::group::${group_title}"
        fi
        local test_report_file="${testng_report_dir}/${failed_test_class}.txt"
        if [ -f "${test_report_file}" ]; then
          local test_report="$(cat "${test_report_file}" | egrep "^Tests run: " | perl -p -se 's/^(Tests run: .*) <<< FAILURE! - in (.*)$/::warning::$report_prefix $2 - $1/' -- -report_prefix="${report_prefix}")"
          echo "$test_report"
          cat "${test_report_file}"
        fi
      done
    fi
  )
}

function retry_failed() {
  if [ "$RESULT" -ne 0 -a ! -d "$MODULE/target/surefire-reports" ]; then
    # No surefire reports and error means compilation error, don't try to rerun
    exit $RESULT
  fi
  if [ "$RESULT" -ne 0 ]; then
    pip install "junitparser==1.4.1"
    $MVN_TEST_COMMAND -pl $MODULE -Dtest="$(python build/failed.py $MODULE/target)" -DtestFailFast=false -DtestForkCount=1
    RESULT=$?
    exit $RESULT
  fi
}

# solution for printing output in "set -x" trace mode without tracing the echo calls
shopt -s expand_aliases
echo_and_restore_trace() {
  builtin echo "$@"
  [ $trace_enabled -eq 1 ] && set -x || true
}
alias echo='{ [[ $- =~ .*x.* ]] && trace_enabled=1 || trace_enabled=0; set +x; } 2> /dev/null; echo_and_restore_trace'

MVN_COMMAND='mvn -B -ntp'
MVN_COMMAND_WITH_RETRY="build/retry.sh ${MVN_COMMAND}"
MVN_TEST_COMMAND="${MVN_COMMAND_WITH_RETRY} test"
MVN_TEST_COMMAND_NO_RETRY="${MVN_COMMAND} test"

echo -n "Test Group : $TEST_GROUP"

# Test Groups  -- start --
function broker_group_1() {
  $MVN_TEST_COMMAND_NO_RETRY -q -pl pulsar-broker -Dgroups='broker'
  RESULT=$?
  MODULE=pulsar-broker
  retry_failed
}

function broker_group_2() {
  $MVN_TEST_COMMAND_NO_RETRY -q -pl pulsar-broker -Dgroups='schema,utils,functions-worker,broker-io,broker-discovery,broker-compaction,broker-naming'
  RESULT=$?
  MODULE=pulsar-broker
  retry_failed
}

function broker_client_api() {
  $MVN_TEST_COMMAND_NO_RETRY -q -pl pulsar-broker -Dgroups='broker-api'
  RESULT=$?
  MODULE=pulsar-broker
  retry_failed
}

function broker_client_impl() {
  $MVN_TEST_COMMAND_NO_RETRY -q -pl pulsar-broker -Dgroups='broker-impl'
  RESULT=$?
  MODULE=pulsar-broker
  retry_failed
}

function broker_flaky() {
  $MVN_TEST_COMMAND_NO_RETRY -q -pl pulsar-broker -Dgroups='flaky,quarantine' -DexcludedGroups=''
  RESULT=$?
  MODULE=pulsar-broker
  retry_failed
 }

function proxy() {
  $MVN_TEST_COMMAND_NO_RETRY -q -pl pulsar-proxy -DexcludedGroups=''
  RESULT=$?
  MODULE=pulsar-proxy
  retry_failed
}

function other() {
  $MVN_COMMAND_WITH_RETRY clean install -PbrokerSkipTest \
                                     -Dexclude='org/apache/pulsar/proxy/**/*.java,
                                                **/ManagedLedgerTest.java,
                                                **/TestPulsarKeyValueSchemaHandler.java,
                                                **/PrimitiveSchemaTest.java,
                                                BlobStoreManagedLedgerOffloaderTest.java'

  $MVN_TEST_COMMAND -pl managed-ledger -Dinclude='**/ManagedLedgerTest.java,
                                                  **/OffloadersCacheTest.java'

  $MVN_TEST_COMMAND -pl pulsar-sql/presto-pulsar-plugin -Dinclude='**/TestPulsarKeyValueSchemaHandler.java'

  $MVN_TEST_COMMAND -pl pulsar-client -Dinclude='**/PrimitiveSchemaTest.java'

  $MVN_TEST_COMMAND -pl tiered-storage/jcloud -Dinclude='**/BlobStoreManagedLedgerOffloaderTest.java'

  echo "::endgroup::"
  local modules_with_quarantined_tests=$(git grep -l '@Test.*"quarantine"' | grep '/src/test/java/' | \
    awk -F '/src/test/java/' '{ print $1 }' | egrep -v 'pulsar-broker|pulsar-proxy' | sort | uniq | \
    perl -0777 -p -e 's/\n(\S)/,$1/g')
  if [ -n "${modules_with_quarantined_tests}" ]; then
    echo "::group::Running quarantined tests outside of pulsar-broker & pulsar-proxy (if any)"
    $MVN_COMMAND -pl "${modules_with_quarantined_tests}" test -Dgroups='quarantine' -DexcludedGroups='' \
      -DfailIfNoTests=false || \
        echo "::warning::There were test failures in the 'quarantine' test group."
    echo "::endgroup::"
  fi
}

# Test Groups  -- end --

TEST_GROUP=$1

echo "Test Group : $TEST_GROUP"

set -x

case $TEST_GROUP in

  BROKER_GROUP_1)
    broker_group_1
    ;;

  BROKER_GROUP_2)
    broker_group_2
    ;;

  BROKER_CLIENT_API)
    broker_client_api
    ;;

  BROKER_CLIENT_IMPL)
    broker_client_impl
    ;;

  BROKER_FLAKY)
    broker_flaky
    ;;

  PROXY)
    proxy
    ;;

  OTHER)
    other
    ;;

  *)
    echo -n "INVALID TEST GROUP"
    exit 1
    ;;
esac
