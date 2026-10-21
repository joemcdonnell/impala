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
import json
import os
from subprocess import check_output, run

from tests.common.base_test_suite import BaseTestSuite


class TestRunWorkload(BaseTestSuite):

  def _generate_run_workload_cmd(self, *extra_args):
    """Returns the command line for a short run of TPCH-Q1 with 'extra_args'."""
    cmd = [
      os.path.join(os.getenv('IMPALA_HOME'), 'bin/run-workload.py'), '-w', 'tpch',
      '--query_names=TPCH-Q1', '--table_format=text/none'] + list(extra_args)
    kerberos_arg = os.getenv('KERB_ARGS')
    if kerberos_arg is not None:
      cmd.append(kerberos_arg)
    return cmd

  def _run_with_profiles(self, tmp_path, *extra_args):
    """Runs TPCH-Q1 three times writing profiles to '[tmp_path]/profiles'. Returns
    the profile directory and the parsed results JSON."""
    profile_dir = tmp_path / 'profiles'
    results_file = tmp_path / 'results.json'
    check_output(self._generate_run_workload_cmd(
        '--query_iterations=3', '--profile_dir={0}'.format(profile_dir),
        '--results_json_file={0}'.format(results_file), *extra_args),
        universal_newlines=True)
    with open(results_file) as f:
      return profile_dir, json.load(f)

  def test_run_workload(self):
    """Test that bin/run-workload.py still works."""
    output = check_output(self._generate_run_workload_cmd(
        '--num_clients=2', '--exec_options=disable_codegen:False'),
        universal_newlines=True)

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

  def test_profile_dir(self, tmp_path):
    """Test that --profile_dir writes a runtime profile for each execution."""
    profile_dir, _ = self._run_with_profiles(tmp_path)
    assert sorted(os.listdir(profile_dir)) == [
        'TPCH-Q1_iter001.txt', 'TPCH-Q1_iter002.txt', 'TPCH-Q1_iter003.txt']
    for name in os.listdir(profile_dir):
      with open(profile_dir / name) as f:
        assert 'Query (id=' in f.read()

  def test_max_profiles(self, tmp_path):
    """Test that --max_profiles keeps the profiles from the first N executions and
    does not affect the results JSON."""
    profile_dir, results = self._run_with_profiles(tmp_path, '--max_profiles=2')
    assert sorted(os.listdir(profile_dir)) == [
        'TPCH-Q1_iter001.txt', 'TPCH-Q1_iter002.txt']
    assert [len(v) for v in results.values()] == [3]

  def test_max_profiles_zero(self, tmp_path):
    """Test that --max_profiles=0 writes no profiles."""
    profile_dir, _ = self._run_with_profiles(tmp_path, '--max_profiles=0')
    assert os.listdir(profile_dir) == []

  def test_max_profiles_invalid(self, tmp_path):
    """Test that invalid --max_profiles usage is rejected before running anything."""
    result = run(self._generate_run_workload_cmd(
        '--max_profiles=-1', '--profile_dir={0}'.format(tmp_path / 'profiles')),
        capture_output=True, universal_newlines=True)
    assert result.returncode != 0
    assert '--max_profiles must not be negative' in result.stderr

  def test_profile_dir_multiple_clients(self, tmp_path):
    """Test that profile file names include the client with multiple clients."""
    profile_dir, results = self._run_with_profiles(tmp_path, '--num_clients=2')
    assert sorted(os.listdir(profile_dir)) == [
        'TPCH-Q1_client{0}_iter{1:03d}.txt'.format(client, i)
        for client in (1, 2) for i in (1, 2, 3)]
    assert [len(v) for v in results.values()] == [6]

  def test_max_profiles_multiple_clients(self, tmp_path):
    """Test that --max_profiles applies to each client separately."""
    profile_dir, results = self._run_with_profiles(
        tmp_path, '--num_clients=2', '--max_profiles=2')
    assert sorted(os.listdir(profile_dir)) == [
        'TPCH-Q1_client{0}_iter{1:03d}.txt'.format(client, i)
        for client in (1, 2) for i in (1, 2)]
    assert [len(v) for v in results.values()] == [6]

  def _run_with_csv(self, tmp_path, *extra_args):
    """Runs TPCH-Q1 with --results_csv_file and returns the header and the rows."""
    csv_file = tmp_path / 'runtimes.csv'
    check_output(self._generate_run_workload_cmd(
        '--results_csv_file={0}'.format(csv_file),
        '--results_json_file={0}'.format(tmp_path / 'results.json'), *extra_args),
        universal_newlines=True)
    with open(csv_file, newline='') as f:
      rows = list(csv.reader(f))
    return rows[0], rows[1:]

  def test_results_csv_file(self, tmp_path):
    """Test that --results_csv_file writes a row per workload iteration with a column
    per query iteration."""
    header, rows = self._run_with_csv(
        tmp_path, '--query_iterations=3', '--workload_iterations=2')
    assert header == ['workload', 'scale_factor', 'table_format', 'query', 'client',
        'workload_iteration', 'iter_1', 'iter_2', 'iter_3']
    assert [row[:6] for row in rows] == [
        ['tpch', '', 'text/none/none', 'TPCH-Q1', '1', str(i)] for i in (1, 2)]
    assert all(float(t) > 0 for row in rows for t in row[6:])

  def test_results_csv_file_multiple_clients(self, tmp_path):
    """Test that --results_csv_file has separate rows for each client and workload
    iteration."""
    header, rows = self._run_with_csv(
        tmp_path, '--query_iterations=2', '--workload_iterations=2', '--num_clients=2')
    assert header[6:] == ['iter_1', 'iter_2']
    assert [row[3:6] for row in rows] == [
        ['TPCH-Q1', str(client), str(i)] for client in (1, 2) for i in (1, 2)]
    assert all(float(t) > 0 for row in rows for t in row[6:])
