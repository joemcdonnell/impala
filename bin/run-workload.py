#!/usr/bin/env impala-python3
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
# This script is used as the driver to run performance benchmarks.
# It does the following:
#   - parses the user defined options and validates them.
#   - Matches the workload to its set of queries and constructs the required objects.
#   - Runs the workload (a combination of dataset and scale factor) against a single
#     table format.
#   - Pretty prints the results of each query's execution.
#   - Stores the execution details in JSON format, keyed by workload and scale factor.
#   - Optionally writes the run times to a CSV file (--results_csv_file), with a row per
#     query, client and workload iteration, and a column per query iteration.
#   - Optionally writes each query execution's runtime profile to a text file in the
#     directory specified by --profile_dir, limited per query (and per client) by
#     --max_profiles.
#

import argparse
import csv
import getpass
import json
import logging
import os
import prettytable

from collections import deque
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from itertools import groupby
from random import shuffle
from sys import exit

from tests.common.test_dimensions import TableFormatInfo
from tests.performance.query import Query, HiveQueryResult
from tests.performance.query_executor import QueryExecConfig
from tests.performance.workload_runner import WorkloadRunner
from tests.performance.workload import Workload
from tests.util.plugin_runner import PluginRunner

parser = argparse.ArgumentParser(
    description="Driver to run performance benchmarks against Impala or Hive.")
parser.add_argument("-v", "--verbose", action="store_true", default=False,
                    help="If set, outputs all benchmark diagnostics.")
parser.add_argument("-w", "--workload", default="tpcds",
                    help=("The workload to run with an optional scale factor "
                          "specified using a colon. For example: -w tpch:400gb. "
                          "Some valid workloads:'tpch', 'tpcds', ..."))
parser.add_argument("--impalads", default="localhost",
                    help=("A comma-separated list of impalad instances to run the "
                          "workload against."))
parser.add_argument("--exec_options", default=str(),
                    help=("Run query exec option string "
                          "(formatted as 'opt1:val1;opt2:val2')."))
parser.add_argument("--results_json_file",
                    default=os.environ['IMPALA_HOME'] + "/benchmark_results.json",
                    help="The output file where benchmark results are saved")
parser.add_argument("--results_csv_file", default=None,
                    help=("If set, the run time of each query execution is written to "
                          "this CSV file. There is a row per query, client id, and "
                          "workload iteration. Each row has a column per query "
                          "iteration. Failed executions are left blank"))
parser.add_argument("--profile_dir", default=None,
                    help=("If set, the runtime profile of each query execution is "
                          "written to a text file in this directory, named "
                          "[query name]_iter[NNN].txt, or "
                          "[query name]_client[C]_iter[NNN].txt if --num_clients is "
                          "greater than 1"))
parser.add_argument("--max_profiles", type=int, default=None,
                    help=("Maximum number of runtime profiles to write per query (per "
                          "client, if --num_clients is greater than 1) when "
                          "--profile_dir is set. The profiles from the first N "
                          "executions of each query are kept. Default: no limit"))
parser.add_argument("-i", "--query_iterations", type=int, default=1,
                    help="Number of times to run each query within a workload")
parser.add_argument("-x", "--workload_iterations", type=int, default=1,
                    help="Number of times to run the workload.")
parser.add_argument("--num_clients", type=int, default=1,
                    help="Number of clients (threads) to use when executing each query.")
parser.add_argument("--query_names", default=str(),
                    help="A comma-separated list of regular expressions. A query is"
                    " executed if it matches any of the expressions.")
parser.add_argument("--table_format", default="parquet/none",
                    help=("The table format to run the workload against. "
                          "Ex. --table_format=seq/snap/block"))
parser.add_argument("--shuffle_query_exec_order", dest="shuffle_queries",
                    action="store_true", default=False,
                    help=("Randomizes the order of query execution. Useful when the "
                          "execution scope is a workload"))
parser.add_argument("--plan_first", action="store_true", default=False,
                    help=("Runs EXPLAIN before running the query so that metadata "
                          "loading is excluded from the timing"))

parser.add_argument("--use_kerberos", action="store_true", default=False,
                    help="If set, enables talking to a kerberized impalad")
parser.add_argument("--continue_on_query_error", action="store_true", default=False,
                    help="If set, continue execution on each query error.")
parser.add_argument("-c", "--client_type", default='hs2', choices=['jdbc', 'hs2'],
                    help="Client type. Valid options are 'jdbc' or 'hs2'")
parser.add_argument("--plugin_names", default=None,
                    help=("Set of comma-separated plugin names with scope; Plugins are"
                          " specified as <plugin_name>[:<scope>]. If no scope if "
                          "specified, it defaults to Query. Plugin names are case "
                          "sensitive"))
parser.add_argument("--exec_engine", default="impala", choices=['impala', 'hive'],
                    help="Which SQL engine to use - impala, hive are valid options")
parser.add_argument("--hiveserver", default="localhost",
                    help="Host that has HiveServers2 service running")
parser.add_argument("--user", default=getpass.getuser(),
                    help="User account under which workload/query will run")
parser.add_argument("--get_password", default=False, action="store_true",
                    help="Prompt for password for user account")
parser.add_argument("--use_ssl", action="store_true", default=False,
                    help="Whether to use SSL or not")

options = parser.parse_args()

options.password = None
if options.get_password:
  options.password = getpass.getpass()
  options.get_password = None

LOG = logging.getLogger('run-workload')


class WorkloadConfig(object):
  """Converts the options dict into a class"""
  def __init__(self, **config):
    self.__dict__.update(config)


class CustomJSONEncoder(json.JSONEncoder):
  """Override the JSONEncoder's default method.

  This class is needed for two reasons:
    - JSON does have a datetime field. We intercept a datetime object and convert it into
      a standard iso string.
    - JSON does not know how to serialize object. We intercept the objects and
      provide their __dict__ representations
  """
  def default(self, obj,):
    if isinstance(obj, Decimal):
      return str(obj)
    if isinstance(obj, datetime):
      # Convert datetime into an standard iso string
      return obj.isoformat()
    if isinstance(obj, bytes):
      # Impyla can leave a string value as bytes when it is unable to decode it to UTF-8.
      # TPC-DS has queries that produce non-UTF-8 results (e.g. Q30 on scale 20)
      # Convert bytes to strings to make JSON encoding work
      return obj.decode(encoding="utf-8", errors="backslashreplace")
    elif isinstance(obj, (Query, HiveQueryResult, QueryExecConfig, TableFormatInfo)):
      # Serialize these objects manually by returning their __dict__ methods.
      return obj.__dict__
    else:
      super(CustomJSONEncoder, self).default(obj)


def prettytable_print(results, failed=False):
  """Print a list of query results in prettytable"""
  column_names = ['Query', 'Start Time', 'Time Taken (s)', 'Client ID']
  if failed: column_names.append('Error')
  table = prettytable.PrettyTable(column_names)
  table.align = 'l'
  table.float_format = '.2'
  # Group the results by table format.
  for table_format_str, gr in groupby(results, lambda x: x.query.table_format_str):
    print("Table Format: %s" % table_format_str)
    for result in gr:
      start_time = result.start_time.strftime("%Y-%m-%d %H:%M:%S") if result.start_time \
          is not None else '-'
      row = [result.query.name, start_time, result.time_taken, result.client_name]
      if failed: row.append(result.query_error)
      table.add_row(row)
    print(table.get_string(sortby='Client ID'))
    table.clear_rows()
    print(str())


def print_result_summary(results):
  """Print failed and successfull queries for a given result list"""
  failed_results = [x for x in results if not x.success]
  successful_results = [x for x in results if x.success]
  prettytable_print(successful_results)
  if failed_results: prettytable_print(failed_results, failed=True)


def write_profiles(results, profile_dir, max_profiles=None, num_clients=1,
                   query_iterations=1):
  """Write the runtime profile of each result to a text file in 'profile_dir'.

  Files are named '[query name]_iter[NNN].txt', where NNN counts the executions of that
  query in the order they appear in 'results'. If 'num_clients' is greater than one,
  the files are named '[query name]_client[C]_iter[NNN].txt' and NNN counts the
  executions of that query by client C. This keeps the names deterministic, because
  the interleaving of the clients' results is not. If 'max_profiles' is not None, only
  the profiles from the first 'max_profiles' executions of each query (by each client)
  are written.
  """
  os.makedirs(profile_dir, exist_ok=True)
  iter_num = {}
  for result in results:
    query_name = result.query.name
    if num_clients > 1:
      query_name = "{}_client{}".format(query_name, result.client_name)
    iter_num[query_name] = iter_num.get(query_name, 0) + 1
    # The counter must agree with the iteration numbers recorded by the scheduler.
    expected_iter = (result.workload_iteration - 1) * query_iterations + \
        result.query_iteration
    assert iter_num[query_name] == expected_iter, (
        "Execution {} of {} is workload iteration {}, query iteration {}".format(
            iter_num[query_name], query_name, result.workload_iteration,
            result.query_iteration))
    if max_profiles is not None and iter_num[query_name] > max_profiles: continue
    file_name = "{}_iter{:03d}.txt".format(query_name, iter_num[query_name])
    # Hive results do not have a runtime profile.
    profile = getattr(result, "runtime_profile", str())
    with open(os.path.join(profile_dir, file_name), "w", encoding="utf-8",
              errors="backslashreplace") as f:
      f.write(profile)


def write_runtimes_csv(results, csv_file, workload_name, scale_factor):
  """Write the run times of the results to 'csv_file'.

  There is one row per (table format, query, client, workload iteration) and the
  columns are 'workload', 'scale_factor', 'table_format', 'query', 'client' and
  'workload_iteration', followed by 'iter_N' for the Nth execution of the query within
  that workload iteration. Executions that failed are left blank.
  """
  runtimes = {}
  for result in results:
    key = (result.query.table_format_str, result.query.name, result.client_name,
        result.workload_iteration)
    # Using a dictionary for the query iterations allows it to work regardless of
    # ordering.
    runtimes.setdefault(key, {})[result.query_iteration] = (
        result.time_taken if result.success else "")
  num_iters = max([max(times) for times in runtimes.values()], default=0)
  with open(csv_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["workload", "scale_factor", "table_format", "query", "client",
        "workload_iteration"] + ["iter_{}".format(i) for i in range(1, num_iters + 1)])
    for key in sorted(runtimes):
      writer.writerow([workload_name, scale_factor] + list(key) +
          [runtimes[key].get(i, "") for i in range(1, num_iters + 1)])


def get_workload_scale_factor():
  """Extract the workload name and scale factor from the command line

  The expected string is "workload[:scale_factor]". Returns a (workload, scale_factor)
  tuple where the scale factor is an empty string if it is not specified.
  """
  workload_tuple = split_and_strip(options.workload, delim=":")
  assert len(workload_tuple) in [1, 2], "Error parsing workload:scale_factor"
  if len(workload_tuple) == 1: workload_tuple.append(str())
  return tuple(workload_tuple)


def split_and_strip(input_string, delim=","):
  """Convert a string into a list using the given delimiter"""
  if not input_string: return list()
  return list(map(str.strip, input_string.split(delim)))


def create_workload_config():
  """Parse command line inputs.

  Some user inputs needs to be transformed from delimited strings to lists in order to be
  consumed by the performacne framework. Additionally, plugin_names are converted into
  objects, and need to be added to the config.
  """
  config = deepcopy(vars(options))
  # We don't need workload and query_names in the config map as they're already specified
  # in the workload object.
  del config['workload']
  del config['query_names']
  config['plugin_runner'] = plugin_runner
  # transform impalads from a string to a list
  impalads = split_and_strip(config['impalads'])
  # Randomize the order of impalads.
  shuffle(impalads)
  config['impalads'] = deque(impalads)
  return WorkloadConfig(**config)


def _validate_options():
  """Basic validation for some commandline options"""
  # the sasl module must be importable on a secure setup.
  if options.use_kerberos: import sasl

  # If Hive is the exec engine, hs2 is the only suported interface.
  if options.exec_engine.lower() == "hive" and options.client_type != "hs2":
    raise RuntimeError("The only supported client type for Hive engine is hs2")

  if options.max_profiles is not None and options.max_profiles < 0:
    raise RuntimeError("--max_profiles must not be negative")

  # The list of Impalads must be provided as a comma separated list of either host:port
  # combination or just host.
  for impalad in split_and_strip(options.impalads):
    if len(impalad.split(":")) not in [1, 2]:
      raise RuntimeError("Impalads must be of the form host:port or host.")


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO, format='[%(name)s]: %(message)s')
  # Check for badly formed user options.
  _validate_options()

  # Intialize the PluginRunner.
  plugin_runner = None
  if options.plugin_names:
    plugin_runner = PluginRunner(split_and_strip(options.plugin_names))

  # Intialize the workload.
  query_name_filters = split_and_strip(options.query_names)
  workload_name, scale_factor = get_workload_scale_factor()
  config = create_workload_config()
  workload = Workload(workload_name, query_name_filters=query_name_filters)
  workload_runner = WorkloadRunner(workload, scale_factor, config)

  # Run the workload
  result_map = dict()
  exit_code = 0
  try:
    if plugin_runner: plugin_runner.run_plugins_pre(scope="Workload")
    workload_runner.run()
    if plugin_runner: plugin_runner.run_plugins_post(scope="Workload")
  finally:
    key = "%s_%s" % (workload_runner.workload.name, workload_runner.scale_factor)
    result_map[key] = workload_runner.results

    if not all(result.success for result in workload_runner.results): exit_code = 1

    # Print the results
    print("\nWorkload: {0}, Scale Factor: {1}\n".format(
        workload_runner.workload.name.upper(), workload_runner.scale_factor))
    print_result_summary(workload_runner.results)

    if options.results_csv_file:
      write_runtimes_csv(workload_runner.results, options.results_csv_file,
                         workload_runner.workload.name, workload_runner.scale_factor)

    if options.profile_dir:
      write_profiles(workload_runner.results, options.profile_dir,
                     options.max_profiles, options.num_clients,
                     options.query_iterations)

  # Store the results
  with open(options.results_json_file, 'w') as f:
    json.dump(result_map, f, cls=CustomJSONEncoder, ensure_ascii=False)

  exit(exit_code)
