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

from __future__ import absolute_import, division, print_function

import re
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_connection import RUNNING

# Matches the profile of each join builder instance, but not the aggregated summary of
# all the instances ("... (join_node_id=2) [3 instances]:").
BUILDER_PROFILE_RE = re.compile(r"Hash Join Builder \(join_node_id=\d+\):")


class TestParallelBroadcastJoinBuild(CustomClusterTestSuite):
  """Tests for the PARALLEL_BROADCAST_JOIN_BUILD query option, which splits the build of
  a broadcast hash join with a shared build across several build fragment instances per
  host, each owning a subset of the join's hash partitions."""

  NUM_HOSTS = 3

  # Joins with a broadcast build side that has a mix of matching and non-matching,
  # unique and duplicate keys.
  QUERIES = [
    # Inner join, many duplicate keys on the build side.
    """select count(*), sum(a.id), sum(b.id) from functional.alltypes a
       join /* +broadcast */ functional.alltypes b on a.int_col = b.int_col
       where a.id < 300 and b.id < 300""",
    # Inner join on a unique key.
    """select count(*), sum(a.id), sum(b.id) from functional.alltypes a
       join /* +broadcast */ functional.alltypes b on a.id = b.id + 1""",
    # Left outer join with unmatched probe rows.
    """select count(*), count(b.id), sum(a.id) from functional.alltypes a
       left outer join /* +broadcast */ functional.alltypestiny b on a.id = b.id""",
    # Left semi and left anti joins.
    """select count(*), sum(id) from functional.alltypes a
       where a.id in (select /* +broadcast */ id from functional.alltypessmall)""",
    """select count(*), sum(id) from functional.alltypes a
       where not exists (select /* +broadcast */ 1 from functional.alltypessmall b
                         where a.id = b.id)""",
    # Null-safe equality, the hash table stores NULLs.
    """select count(*) from functional.alltypesagg a
       join /* +broadcast */ functional.alltypesagg b
       on a.tinyint_col <=> b.tinyint_col where a.id < 500 and b.id < 500""",
    # Empty build side.
    """select count(*) from functional.alltypes a
       join /* +broadcast */ functional.alltypes b on a.id = b.id and b.id < 0""",
  ]

  def _num_builders(self, profile):
    return len(BUILDER_PROFILE_RE.findall(profile))

  # Aggregated per-filter scan statistics, e.g.
  #   Filter 0 (1.00 MB) [6 instances]:
  #      - Rows processed: 51 (51)
  #      - Rows rejected: 50 (50)
  AGGREGATED_FILTER_RE = re.compile(
      r"Filter (\d+) \([^)]*\) \[\d+ instances\]:\n((?:[ ]+- .*\n)+)")

  def _rows_rejected_by_filter(self, profile):
    """Returns a map from filter id to the total rows rejected by that filter."""
    rejected = {}
    for filter_id, stats in self.AGGREGATED_FILTER_RE.findall(profile):
      match = re.search(r"Rows rejected: (\d+)", stats)
      if match:
        rejected[filter_id] = rejected.get(filter_id, 0) + int(match.group(1))
    return rejected

  def _run(self, query, num_build_workers, runtime_filters, extra_options=None):
    self.client.set_configuration_option("mt_dop", "2")
    self.client.set_configuration_option("parallel_broadcast_join_build",
        str(num_build_workers))
    self.client.set_configuration_option("runtime_filter_mode",
        "GLOBAL" if runtime_filters else "OFF")
    self.client.set_configuration_option("exec_single_node_rows_threshold", "0")
    # Make sure the scans wait for the filter, so that the same rows are rejected.
    self.client.set_configuration_option("runtime_filter_wait_time_ms", "60000")
    for name, value in (extra_options or {}).items():
      self.client.set_configuration_option(name, value)
    result = self.client.execute(query)
    return result, result.runtime_profile

  @CustomClusterTestSuite.with_args(cluster_size=NUM_HOSTS)
  def test_results_match_serial_build(self):
    """The results of joins with a parallel build match those with the serial build, and
    the parallel build actually used several build instances per host."""
    for runtime_filters in [False, True]:
      for query in self.QUERIES:
        expected, serial_profile = self._run(query, 0, runtime_filters)
        assert self._num_builders(serial_profile) == self.NUM_HOSTS, query
        for num_workers in [2, 4, 16]:
          actual, profile = self._run(query, num_workers, runtime_filters)
          assert actual.data == expected.data, \
              "{0} workers, filters={1}: {2}".format(
                  num_workers, runtime_filters, query)
          assert self._num_builders(profile) == self.NUM_HOSTS * num_workers, query

  @CustomClusterTestSuite.with_args(cluster_size=NUM_HOSTS)
  def test_null_aware_anti_join_not_parallel(self):
    """A null-aware anti join needs a single builder and ignores the option."""
    query = """select count(*) from functional.alltypes a where a.id not in
               (select /* +broadcast */ id from functional.alltypessmall)"""
    expected, _ = self._run(query, 0, False)
    actual, profile = self._run(query, 4, False)
    assert actual.data == expected.data
    assert self._num_builders(profile) == self.NUM_HOSTS

  # The probe side scans a parquet table, so that row-level runtime filtering applies.
  FILTER_QUERIES = [
    # Build keys spread over all hash partitions, many probe rows are rejected.
    """select straight_join count(*), sum(a.id) from functional_parquet.alltypes a
       join /* +broadcast */ functional_parquet.alltypessmall b on a.id = b.id""",
    # Build keys from a narrow range, which makes the min/max filter selective.
    """select straight_join count(*), sum(a.id) from functional_parquet.alltypes a
       join /* +broadcast */ functional_parquet.alltypestiny b on a.id = b.id""",
    # Duplicate keys on the build side, from a selective subset of the probe keys.
    """select straight_join count(*) from functional_parquet.alltypes a
       join /* +broadcast */
         (select id from functional_parquet.alltypes where id between 10 and 40
          union all
          select id from functional_parquet.alltypes where id between 20 and 60) b
       on a.id = b.id""",
  ]

  @CustomClusterTestSuite.with_args(cluster_size=NUM_HOSTS)
  def test_bloom_filters_effective(self):
    """The bloom filters published by a parallel build are as selective as the ones
    from the serial build: merging the partial filters of the workers loses no keys
    (results match) and adds no false positives (the same rows are rejected)."""
    options = {"enabled_runtime_filter_types": "BLOOM"}
    for query in self.FILTER_QUERIES:
      expected, serial_profile = self._run(query, 0, True, options)
      serial_rejected = self._rows_rejected_by_filter(serial_profile)
      assert sum(serial_rejected.values()) > 0, \
          "Test query does not reject any rows: {0}".format(query)
      for num_workers in [2, 4, 16]:
        actual, profile = self._run(query, num_workers, True, options)
        assert actual.data == expected.data, query
        assert self._num_builders(profile) == self.NUM_HOSTS * num_workers, query
        assert self._rows_rejected_by_filter(profile) == serial_rejected, \
            "{0} workers reject different rows than the serial build: {1}".format(
                num_workers, query)

  # Min/max filters are generated for Kudu scans. Kudu applies them itself, so they do
  # not show up as rejected rows, but they reduce the rows returned by the scan.
  MIN_MAX_QUERIES = [
    """select straight_join count(*), sum(a.id) from functional_kudu.alltypes a
       join /* +broadcast */ functional_kudu.alltypessmall b on a.id = b.id""",
    """select straight_join count(*) from functional_kudu.alltypes a
       join /* +broadcast */
         (select id from functional_kudu.alltypes where id between 10 and 40
          union all
          select id from functional_kudu.alltypes where id between 20 and 60) b
       on a.id = b.id""",
  ]

  PROBE_SCAN_NODE_RE = re.compile(r"^\s*\w+_SCAN_NODE \(id=0\)")
  ROWS_RETURNED_RE = re.compile(r"^\s*- RowsReturned: (\d+)")

  def _probe_scan_rows_returned(self, profile):
    """Returns the total rows returned by the instances of the probe scan, node 0."""
    total = 0
    in_scan = False
    for line in profile.split("\n"):
      if self.PROBE_SCAN_NODE_RE.match(line):
        in_scan = True
      elif in_scan:
        match = self.ROWS_RETURNED_RE.match(line)
        if match:
          total += int(match.group(1))
          in_scan = False
    return total

  @CustomClusterTestSuite.with_args(cluster_size=NUM_HOSTS)
  def test_min_max_filters_effective(self):
    """The min/max filters published by a parallel build are the same as the serial
    build's: the probe scan returns the same number of rows."""
    options = {"enabled_runtime_filter_types": "MIN_MAX"}
    for query in self.MIN_MAX_QUERIES:
      expected, serial_profile = self._run(query, 0, True, options)
      serial_rows = self._probe_scan_rows_returned(serial_profile)
      # The probe table has 7300 rows. The filter must cut that down.
      assert 0 < serial_rows < 1000, \
          "Test query is not selective ({0} rows): {1}".format(serial_rows, query)
      for num_workers in [2, 4, 16]:
        actual, profile = self._run(query, num_workers, True, options)
        assert actual.data == expected.data, query
        assert self._num_builders(profile) == self.NUM_HOSTS * num_workers, query
        assert self._probe_scan_rows_returned(profile) == serial_rows, \
            "{0} workers return different scan rows than the serial build: {1}".format(
                num_workers, query)

  @CustomClusterTestSuite.with_args(cluster_size=NUM_HOSTS)
  def test_in_list_filters_correct(self):
    """In-list filters are not merged yet, so a parallel build disables them. The
    results must still be correct."""
    query = self.FILTER_QUERIES[0]
    options = {"enabled_runtime_filter_types": "IN_LIST"}
    expected, _ = self._run(query, 0, True, options)
    for num_workers in [2, 4]:
      actual, _ = self._run(query, num_workers, True, options)
      assert actual.data == expected.data


class TestParallelBroadcastJoinBuildFailures(CustomClusterTestSuite):
  """Tests for cancelling and failing queries that use a parallel broadcast join build.
  The build workers of a host wait for each other and for the probes, so a hang here
  would leave fragment instances running forever."""

  NUM_HOSTS = 3
  NUM_WORKERS = 4

  QUERY = """select count(*), sum(a.id), sum(b.id) from functional.alltypes a
             join /* +broadcast */ functional.alltypes b on a.id = b.id + 1"""

  def _set_options(self, debug_action):
    self.client.set_configuration_option("mt_dop", "2")
    self.client.set_configuration_option("parallel_broadcast_join_build",
        str(self.NUM_WORKERS))
    self.client.set_configuration_option("exec_single_node_rows_threshold", "0")
    self.client.set_configuration_option("debug_action", debug_action)

  def _expected_result(self):
    self._set_options("")
    self.client.set_configuration_option("parallel_broadcast_join_build", "0")
    return self.client.execute(self.QUERY).data

  def _wait_for_no_fragments(self):
    """Fails if any fragment instance, including the build workers, does not finish."""
    for impalad in self.cluster.impalads:
      impalad.service.wait_for_metric_value(
          "impala-server.num-fragments-in-flight", 0, timeout=90)

  def _check_cluster_healthy(self, expected):
    """All the impalads are still running, and a parallel build still works."""
    self._wait_for_no_fragments()
    self._set_options("")
    result = self.client.execute(self.QUERY)
    assert result.data == expected
    assert len(BUILDER_PROFILE_RE.findall(result.runtime_profile)) \
        == self.NUM_HOSTS * self.NUM_WORKERS

  def _cancel_after_start(self, debug_action, delay_s, wait_for_running=True):
    self._set_options(debug_action)
    handle = self.client.execute_async(self.QUERY)
    if wait_for_running:
      self.client.wait_for_impala_state(handle, RUNNING, 30)
    time.sleep(delay_s)
    self.client.cancel(handle)
    self.client.close_query(handle)

  @CustomClusterTestSuite.with_args(cluster_size=NUM_HOSTS)
  def test_cancel_while_leader_waits_for_workers(self):
    """Cancel while the leaders wait for workers that are delayed. The leaders must wake
    up, and the workers that finish later must not wait for anything."""
    expected = self._expected_result()
    self._cancel_after_start("PHJ_BUILDER_WORKER_FINISH:SLEEP@5000", 1)
    self._check_cluster_healthy(expected)

  @CustomClusterTestSuite.with_args(cluster_size=NUM_HOSTS)
  def test_cancel_while_probing(self):
    """Cancel after the build was handed to the probes. The workers wait in Close() for
    the leader, which waits for the probes."""
    expected = self._expected_result()
    # The hash join is node 2: scans are 0 and 1. Its GetNext() blocks, after the probes
    # got the build, until the query is cancelled.
    self._cancel_after_start("2:GETNEXT:WAIT", 1)
    self._check_cluster_healthy(expected)

  @CustomClusterTestSuite.with_args(cluster_size=NUM_HOSTS)
  def test_cancel_at_random_times(self):
    """Cancel at a range of delays, to hit different stages of the build. The query runs
    for a few tens of milliseconds, so some cancels arrive after it finished."""
    expected = self._expected_result()
    for _ in range(3):
      for delay_s in [0, 0.005, 0.01, 0.02, 0.03, 0.05, 0.08]:
        self._cancel_after_start("", delay_s, wait_for_running=False)
    self._check_cluster_healthy(expected)

  @CustomClusterTestSuite.with_args(cluster_size=NUM_HOSTS)
  def test_worker_failure(self):
    """Workers that fail must fail the query without leaving the leaders waiting."""
    expected = self._expected_result()
    # All the workers fail.
    self._set_options("PHJ_BUILDER_WORKER_FINISH:FAIL@1.0")
    try:
      self.client.execute(self.QUERY)
      assert False, "Query should have failed"
    except Exception as e:
      assert "Debug Action: PHJ_BUILDER_WORKER_FINISH:FAIL" in str(e), str(e)
    # Some of the workers fail, the other ones have already told the leader that they
    # are done, and wait for it to release them.
    for _ in range(5):
      self._set_options("PHJ_BUILDER_WORKER_FINISH:FAIL@0.3")
      try:
        assert self.client.execute(self.QUERY).data == expected
      except Exception as e:
        assert "Debug Action: PHJ_BUILDER_WORKER_FINISH:FAIL" in str(e), str(e)
    self._check_cluster_healthy(expected)

  @CustomClusterTestSuite.with_args(cluster_size=NUM_HOSTS)
  def test_leader_failure_after_takeover(self):
    """The leaders fail after they took over the partitions of the workers. The workers
    are blocked in Close() until the leader releases them."""
    expected = self._expected_result()
    self._set_options("PHJ_BUILDER_LEADER_FINISH:FAIL@1.0")
    try:
      self.client.execute(self.QUERY)
      assert False, "Query should have failed"
    except Exception as e:
      assert "Debug Action: PHJ_BUILDER_LEADER_FINISH:FAIL" in str(e), str(e)
    self._check_cluster_healthy(expected)
