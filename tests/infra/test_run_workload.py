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

import csv
import os
from subprocess import check_output

from tests.common.base_test_suite import BaseTestSuite


class TestRunWorkload(BaseTestSuite):

  def _run_workload(self, extra_args):
    """Runs bin/run-workload.py on the tpch workload with 'extra_args' and returns
    its stdout."""
    impala_home = os.getenv('IMPALA_HOME')
    cmd = [
      os.path.join(impala_home, 'bin/run-workload.py'), '-w', 'tpch',
      '--table_format=text/none'] + extra_args
    kerberos_arg = os.getenv('KERB_ARGS')
    if kerberos_arg is not None:
      cmd.append(kerberos_arg)
    return check_output(cmd, universal_newlines=True)

  def test_run_workload(self):
    """Test that bin/run-workload.py still works."""
    output = self._run_workload([
      '--num_clients=2', '--query_names=TPCH-Q1',
      '--exec_options=disable_codegen:False'])

    """
    Full stdout is like this:

    Workload: TPCH, Scale Factor:

    Table Format: text/none/none
    +---------+---------------------+----------------+-----------+
    | Query   | Start Time          | Time Taken (s) | Client ID |
    +---------+---------------------+----------------+-----------+
    | TPCH-Q1 | 2025-01-27 15:40:28 | 5.59           | 1         |
    | TPCH-Q1 | 2025-01-27 15:40:28 | 5.65           | 2         |
    +---------+---------------------+----------------+-----------+
    """
    assert "Workload: TPCH, Scale Factor:" in output
    assert "Table Format: text/none/none" in output
    assert "Query" in output
    assert "Start Time" in output
    assert "Time Taken (s)" in output
    assert "Client ID" in output
    assert "TPCH-Q1" in output

  def test_results_csv(self, tmp_path):
    """Test that --results_csv_file writes one row per query with one column per run."""
    csv_file = tmp_path / "results.csv"
    self._run_workload([
      '--query_names=TPCH-Q1,TPCH-Q6', '-i', '2',
      '--results_json_file=' + str(tmp_path / "results.json"),
      '--results_csv_file=' + str(csv_file)])

    with open(str(csv_file), newline='') as f:
      rows = list(csv.reader(f))
    assert rows[0] == ['query', 'run_1', 'run_2']
    assert sorted(row[0] for row in rows[1:]) == ['TPCH-Q1', 'TPCH-Q6']
    for row in rows[1:]:
      assert len(row) == 3
      assert all(float(time_taken) > 0 for time_taken in row[1:])

  def test_profile_dir(self, tmp_path):
    """Test that --profile_dir creates the directory and writes one profile file per
    query run."""
    profile_dir = tmp_path / "profiles"
    self._run_workload([
      '--query_names=TPCH-Q1', '-i', '2',
      '--results_json_file=' + str(tmp_path / "results.json"),
      '--profile_dir=' + str(profile_dir)])

    assert sorted(os.listdir(str(profile_dir))) == [
      'TPCH-Q1_iter001.txt', 'TPCH-Q1_iter002.txt']
    for file_name in os.listdir(str(profile_dir)):
      with open(str(profile_dir / file_name)) as f:
        assert "Query (id=" in f.read()
