#!/bin/bash
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
# Runner script for the perf-ab-test job using cgroups. This runs
# the bin/single_node_perf_run.py script inside a systemd service
# so it can isolate Impalads to separate CPUs.

set -euo pipefail
. "$IMPALA_HOME"/bin/report_build_error.sh
setup_report_build_error

# Start time of run.
START_TIME=$(date +"%Y-%m-%d %H:%M:%S")

cd "${IMPALA_HOME}"

RET_CODE=0
if ! bin/bootstrap_system.sh; then
  RET_CODE=1
fi

source bin/impala-config.sh > /dev/null 2>&1

if [[ $RET_CODE == 0 ]]; then
  if ! ./buildall.sh -notests -release -format ; then
    RET_CODE=1
  fi
fi

# Define optional variables
: ${CUSTOM_DATALOAD_SCRIPT:=}
: ${START_IMPALA_CLUSTER_ARGS:=}

if [[ -n "${CUSTOM_DATALOAD_SCRIPT}" ]]; then
  DATALOAD_ARGS="--custom_dataload_script ${CUSTOM_DATALOAD_SCRIPT}"
else
  DATALOAD_ARGS="--load"
fi

if [[ -n "${START_IMPALA_CLUSTER_ARGS}" ]]; then
  START_IMPALA_CLUSTER_ARGS="--start_impala_cluster_args='${START_IMPALA_CLUSTER_ARGS}'"
else
  START_IMPALA_CLUSTER_ARGS=""
fi

# Build the runner script, systemd service, and reload the daemon.
# This does not start the service.
function create_systemd_service {
  # Build a runner script for single_node_perf_run.py to run via
  # a systemd service. Variables are replaced.
cat <<EOF > perf-ab-test-runner.sh
#!/bin/bash

cd ${IMPALA_HOME}
source bin/impala-config.sh

# This needs to exec so there is no dangling process in the root cgroup
# from this runner script.
exec bin/single_node_perf_run.py --iterations "${ITERATIONS}" \
  --scale "${SCALE}" --table_formats "${TABLE_FORMATS}" \
  --workload "${WORKLOAD}" --num_impalads "${NUM_IMPALADS}" \
  --query_names "$QUERY_NAMES" --load --start_minicluster \
  --impalad_args '$IMPALAD_ARGS' "${HASH_A}" "${HASH_B}" \
  --cpus_per_impalad ${CPUS_PER_IMPALAD} --use_cgroup_cpusets \
  ${DATALOAD_ARGS} ${START_IMPALA_CLUSTER_ARGS}
EOF

  chmod +x perf-ab-test-runner.sh

  # For debugging, print the script
  cat perf-ab-test-runner.sh

  # Build the service file for running perf-ab-test
  # This runs as the current user, uses delegation so we can create subgroups,
  # and specifies that we run on all the CPUs. Variables are replaced.
cat <<EOF > perf-ab-test.service
[Unit]
Description=Perf-AB-test Runner Service

[Service]
Type=simple
User=${USER}
Group=${USER}
WorkingDirectory=${IMPALA_HOME}
ExecStart=${IMPALA_HOME}/perf-ab-test-runner.sh
Delegate=yes
AllowedCPUs=$(cat /sys/fs/cgroup/cpuset.cpus.effective)
AllowedMemoryNodes=$(cat /sys/fs/cgroup/cpuset.mems.effective)

[Install]
WantedBy=multi-user.target

EOF

  # For debugging, print the service file
  cat perf-ab-test.service

  # Use sudo to move it into place
  sudo mv perf-ab-test.service /etc/systemd/system/
  # Reload systemd so it sees the new service
  sudo systemctl daemon-reload
}

# Wait for the perf-ab-test systemd service to exit
function wait_for_systemd_service_exit {
  # systemctl is-active has different possible states for our service (which will exit
  # at some point):
  # Terminal: inactive, failed
  # Non-terminal: active, activating, deactivating, maintenance
  # Let's wait for a terminal state.
  # TODO: This could be extended to add a time limit
  while true ; do
    # systemctl will return a non-zero return code once the service is not active
    # Wrap the command with echo to ignore the return code
    SYSTEMD_STATE=$(echo $(systemctl is-active perf-ab-test))
    if [[ "${SYSTEMD_STATE}" == "inactive" || "${SYSTEMD_STATE}" == "failed" ]]; then
      break;
    fi
    sleep 5
  done
}

# Remove the perf-ab-test service added by create_systemd_service
function delete_systemd_service {
  sudo rm /etc/systemd/system/perf-ab-test.service
  rm perf-ab-test-runner.sh
  sudo systemctl daemon-reload
}

# Skip running the perf test if the build failed
if [[ $RET_CODE == 0 ]]; then
  # We run bin/single_node_perf_run.py script via a systemd service so it is in a cgroup
  # Create the systemd service and start it
  create_systemd_service
  sudo systemctl start perf-ab-test

  # Fork off something to monitor the logs. This mimics the output from simply running the
  # script directly. "-o cat" avoids prepending extra timestamps / unit information
  echo "Start monitoring the logs"
  journalctl -u perf-ab-test -o cat -f &
  LOG_MONITOR_PID=$!

  echo "Waiting for the perf-ab-test service to exit..."
  wait_for_systemd_service_exit

  # Print the status for debuggability. Wrap this with echo as systemctl status can have
  # a non-zero return code.
  echo "$(systemctl status perf-ab-test)"
  # Check the service return code and incorporate it into RET_CODE
  SYSTEMD_RET_CODE=$(systemctl show perf-ab-test --property=ExecMainStatus --value)
  if [[ ${SYSTEMD_RET_CODE} != 0 ]]; then
    echo "perf-ab-test service failed! Return code: ${SYSTEMD_RET_CODE}"
    RET_CODE=${SYSTEMD_RET_CODE}
  fi

  # Kill the log monitor pid
  kill $LOG_MONITOR_PID
  echo "Stop monitoring the logs"

  delete_systemd_service
fi

# Always shutdown minicluster at the end and run finalize.sh
testdata/bin/kill-all.sh
bin/jenkins/finalize.sh "${START_TIME}"
exit "$RET_CODE"
