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

# Usage:
# single_node_perf_run.py [options] git_hash_A [git_hash_B]
#
# When one hash is given, measures the performance on the specified workloads.
# When two hashes are given, compares their performance. Output is in
# $IMPALA_HOME/perf_results/latest. In the performance_result_*.txt files,
# git_hash_A is referred to as the "Base" result. For example, if you run with
# git_hash_A = aBad1dea... and git_hash_B = 8675309... the
# performance_result_*.txt will say at the top:
#
#   Run Description: "aBad1dea... vs 8675309..."
#
# The different queries will have their run time statistics in columns
# "Avg(s)", "StdDev(%)", "BaseAvg(s)", "Base StdDev(%)". The first two refer
# to git_hash_B, the second two refer to git_hash_A. The column "Delta(Avg)"
# is negative if git_hash_B is faster and is positive if git_hash_A is faster.
#
# To run this script against data stored in Kudu, set '--table_formats=kudu/none/none'.
#
# For a given workload, the target database used will be:
# '[workload-name][scale-factor]_[table_format]'. Typically, on the first run of this
# script the target database will not exist. The --load option will be needed to load
# the database.
#
# WARNING: This script will run git checkout. You should not touch the tree
# while the script is running. You should start the script from a clean git
# tree.
#
# WARNING: When --load is used, this script calls load_data.py which can
# overwrite your TPC-H and TPC-DS data.
#
# WARNING: This script does not respect any existing environment variables. Environment
# variables should be placed into bin/impala-config-local.sh to have an effect.
#
# Each combination of workload and table format is run and reported separately. The
# filenames use a workload name based on the workload and table_format to keep the
# files separate.
#
# Options:
#   -h, --help            show this help message and exit
#   --workloads=WORKLOADS
#                         comma-separated list of workloads. Choices: tpch,
#                         targeted-perf, tpcds. Default: targeted-perf
#   --scale=SCALE         scale factor for the workloads [required]
#   --iterations=ITERATIONS
#                         number of times to run each query
#   --table_formats=TABLE_FORMATS
#                         comma-separated list of table formats. Default:
#                         parquet/none
#   --num_impalads=NUM_IMPALADS
#                         number of executor impalads. Default: 1
#   --num_coordinators=NUM_COORDINATORS
#                         number of coordinators. Without
#                         --use_exclusive_coordinators, the coordinators are
#                         also executors and must not exceed --num_impalads.
#                         Default: 1
#   --use_exclusive_coordinators
#                         coordinators do not execute fragments. They are
#                         started in addition to --num_impalads executors.
#   --query_names=QUERY_NAMES
#                         comma-separated list of regular expressions. A query
#                         is executed if it matches any regular expression in
#                         this list
#   --load                load databases for the chosen workloads
#   --start_minicluster   start a new Hadoop minicluster
#   --ninja               use ninja, rather than Make, as the build tool
#   --exec_options        query exec option string to run workload
#                         (formatted as 'opt1:val1;opt2:val2')
#   --max_profiles=MAX_PROFILES
#                         maximum number of runtime profiles to keep per query
#                         (the first N iterations). Default: no limit

from tempfile import mkdtemp

import argparse
import os
import pipes
import shutil
import subprocess
import textwrap

from tests.common.test_dimensions import TableFormatInfo

IMPALA_HOME = os.environ["IMPALA_HOME"]
IMPALA_PERF_RESULTS = os.path.join(IMPALA_HOME, "perf_results")


def configured_call(cmd):
  """Call a command in a shell with config-impala.sh."""
  if type(cmd) is list:
    cmd = " ".join([pipes.quote(arg) for arg in cmd])
  cmd = "source {0}/bin/impala-config.sh && {1}".format(IMPALA_HOME, cmd)
  # Set env={} to use a clean environment. Existing environment variables can cause
  # complicated interactions. For example, this script runs with impala-python3,
  # which sets LD_LIBRARY_PATH to use the toolchain libstdc++. On newer OSes, this
  # can cause issues for system binaries that need to use a newer libstdc++.
  return subprocess.check_call(["bash", "-c", cmd], env={})


def run_git(args):
  """Runs git without capturing output (stdout passes through to stdout)"""
  subprocess.check_call(["git"] + args, text=True, env={})


def get_git_output(args):
  """Runs git, capturing the output and returning it"""
  return subprocess.check_output(["git"] + args, text=True, env={})


def load_data(db_to_load, table_formats, scale):
  """Loads a database with a particular scale factor."""
  all_formats = ("text/none," + table_formats if "text/none" not in table_formats
                 else table_formats)
  configured_call(["{0}/bin/load-data.py".format(IMPALA_HOME),
                   "--workloads", db_to_load, "--scale_factor", str(scale),
                   "--table_formats", all_formats])
  for table_format in table_formats.split(","):
    suffix = TableFormatInfo.create_from_string(None, table_format).db_suffix()
    db_name = db_to_load + scale + suffix
    configured_call(["{0}/tests/util/compute_table_stats.py".format(IMPALA_HOME),
                     "--stop_on_error", "--db_names", db_name,
                     "--parallelism", "1"])


def get_git_hash_for_name(name):
  return get_git_output(["rev-parse", name]).strip()


def build(git_hash, options):
  """Builds Impala in release mode; doesn't build tests."""
  run_git(["checkout", git_hash])
  buildall = ["{0}/buildall.sh".format(IMPALA_HOME), "-notests", "-release", "-noclean"]
  if options.ninja:
    buildall += ["-ninja"]
  configured_call(buildall)


def start_minicluster():
  configured_call(["{0}/bin/create-test-configuration.sh".format(IMPALA_HOME)])
  configured_call(["{0}/testdata/bin/run-all.sh".format(IMPALA_HOME)])


def get_cluster_size(options):
  """Returns the total number of impalads to start. Exclusive coordinators are in
  addition to the --num_impalads executors."""
  if options.use_exclusive_coordinators:
    return options.num_impalads + options.num_coordinators
  return options.num_impalads


def start_impala(options):
  cmd = ["{0}/bin/start-impala-cluster.py".format(IMPALA_HOME), "-s",
         str(get_cluster_size(options)), "-c", str(options.num_coordinators)]
  if options.use_exclusive_coordinators:
    cmd += ["--use_exclusive_coordinators"]
  configured_call(
      cmd + ["--impalad_args={0}".format(arg) for arg in options.impalad_args])


def get_combinations(options):
  """Returns a list of (workload, table_format, workload_name) for each combination of
  the requested workloads and table formats. 'name' is suitable for use in file names."""
  return [(workload, table_format,
           "{0}_{1}".format(workload, table_format.replace("/", "_")))
          for workload in options.workloads.split(",")
          for table_format in options.table_formats.split(",")]


def get_workload_run_id(phase, git_hash, workload_name):
  # For filenames, the full git hash is noisy, so shorten it to 10 chars
  short_git_hash = git_hash[:10]
  return "_".join([phase, short_git_hash, workload_name])


def run_workload(base_dir, phase, git_hash, workload, table_format, workload_name,
                 options):
  """Runs a single combination of workload and table format with the given options.

  The 'workload_name' identifies the combination and is incorporated into the
  output filenames and directories
  (e.g. ${base_dir}/[phase]_[git_hash]_[workload_name].json")
  """
  impalads = ",".join(["localhost:{0}".format(21050 + i)
                       for i in range(0, options.num_coordinators)])

  workload_run_id = get_workload_run_id(phase, git_hash, workload_name)
  cmd = ["{0}/bin/run-workload.py".format(IMPALA_HOME)]
  cmd += ["--workload={0}:{1}".format(workload, options.scale),
          "--impalads={0}".format(impalads),
          "--results_json_file={0}/{1}.json".format(base_dir, workload_run_id),
          "--profile_dir={0}/{1}_profiles".format(base_dir, workload_run_id),
          "--query_iterations={0}".format(options.iterations),
          "--table_format={0}".format(table_format),
          "--plan_first"]

  if options.exec_options:
    cmd += ["--exec_options={0}".format(options.exec_options)]

  if options.query_names:
    cmd += ["--query_names={0}".format(options.query_names)]

  if options.max_profiles is not None:
    cmd += ["--max_profiles={0}".format(options.max_profiles)]

  configured_call(cmd)


def run_workloads(base_dir, phase, options):
  """Runs each combination of workload and table format with the given options.

  Each run writes its results to '[base_dir]/[phase]_[git hash]_[workload_name].json'.
  Phase is 'a' or 'b'.
  """
  git_hash = get_git_hash_for_name("HEAD")
  for workload, table_format, workload_name in get_combinations(options):
    run_workload(base_dir, phase, git_hash, workload, table_format, workload_name,
                 options)


def report_benchmark_results(file_a, file_b, description, workload_name):
  """Wrapper around report_benchmark_result.py."""
  # This is using impala-python3, so it can tolerate inheriting the environment.
  performance_result = subprocess.check_output(
    ["{0}/tests/benchmark/report_benchmark_results.py".format(IMPALA_HOME),
     "--reference_result_file={0}".format(file_a),
     "--input_result_file={0}".format(file_b),
     '--report_description="{0}"'.format(description)],
    text=True)

  # Output the performance result to stdout for convenience
  print(performance_result)

  # Dump the performance result to a file to preserve
  result = os.path.join(IMPALA_PERF_RESULTS, "latest",
                        "performance_result_{0}.txt".format(workload_name))
  with open(result, "w") as f:
    f.write(performance_result)


def compare(base_dir, hash_a, hash_b, workload_name):
  """Take the results of two performance runs for one combination of workload and table
  format (identified by 'workload_name') and compare them."""
  a_workload_run_id = get_workload_run_id("a", hash_a, workload_name)
  b_workload_run_id = get_workload_run_id("b", hash_b, workload_name)
  file_a = os.path.join(base_dir, a_workload_run_id + ".json")
  file_b = os.path.join(base_dir, b_workload_run_id + ".json")
  description = "{0} vs {1}".format(hash_a, hash_b)
  report_benchmark_results(file_a, file_b, description, workload_name)

  # Diff the runtime profiles written by run-workload.py
  diff_file = os.path.join(
      IMPALA_HOME, "performance_result_profile_diff_{0}.txt".format(workload_name))
  with open(diff_file, "w") as f:
    # This does not check that the diff command succeeds
    a_profile_dir = os.path.join(base_dir, a_workload_run_id + "_profiles")
    b_profile_dir = os.path.join(base_dir, b_workload_run_id + "_profiles")
    subprocess.run(["diff", "-u", os.path.join(base_dir, a_profile_dir, b_profile_dir)],
      stdout=f, text=True, env={})


def compare_all(base_dir, hash_a, hash_b, options):
  """Take the results of two performance runs and compare them.

  Each combination of workload and table format is compared and reported separately.
  """
  for _, _, workload_name in get_combinations(options):
    compare(base_dir, hash_a, hash_b, workload_name)


def backup_workloads():
  """Copy the workload folder to a temporary directory and returns its name.

  Used to keep workloads from being clobbered by git checkout.
  """
  temp_dir = mkdtemp()
  shutil.copytree(os.path.join(IMPALA_HOME, "testdata", "workloads"),
                  os.path.join(temp_dir, "workloads"))
  print("Backed up workloads to {0}".format(temp_dir))
  return temp_dir


def restore_workloads(source):
  """Restores the workload directory from source into the Impala tree."""
  # dirs_exist_ok=True allows this to overwrite the existing files
  shutil.copytree(os.path.join(source, "workloads"),
                  os.path.join(IMPALA_HOME, "testdata", "workloads"), dirs_exist_ok=True)


def perf_ab_test(options, args):
  """Does the main work: build, run tests, compare."""
  hash_a = get_git_hash_for_name(args[0])

  # Create the base directory to store the results in
  results_path = IMPALA_PERF_RESULTS
  if not os.access(results_path, os.W_OK):
    os.makedirs(results_path)

  temp_dir = mkdtemp(dir=results_path, prefix="perf_run_")
  latest = os.path.join(results_path, "latest")
  if os.path.islink(latest):
    os.remove(latest)
  os.symlink(os.path.basename(temp_dir), latest)
  workload_dir = backup_workloads()

  build(hash_a, options)
  restore_workloads(workload_dir)

  if options.start_minicluster:
    start_minicluster()
  start_impala(options)

  workloads = options.workloads.split(",")

  if options.load:
    WORKLOAD_TO_DATASET = {
      "tpch": "tpch",
      "tpcds": "tpcds",
      "targeted-perf": "tpch",
      "tpcds-unmodified": "tpcds-unmodified",
      "tpcds_partitioned": "tpcds_partitioned"
    }
    datasets = [WORKLOAD_TO_DATASET[workload] for workload in workloads]
    if "tpcds_partitioned" in datasets and "tpcds" not in datasets:
      # "tpcds_partitioned" require the text "tpcds" database.
      load_data("tpcds", "text/none", options.scale)
    for dataset in datasets:
      load_data(dataset, options.table_formats, options.scale)

  run_workloads(temp_dir, "a", options)

  if len(args) > 1 and args[1]:
    hash_b = get_git_hash_for_name(args[1])
    # discard any changes created by the previous restore_workloads()
    shutil.rmtree("testdata/workloads")
    run_git(["checkout", "--", "testdata/workloads"])
    build(hash_b, options)
    restore_workloads(workload_dir)
    start_impala(options)
    run_workloads(temp_dir, "b", options)
    compare_all(temp_dir, hash_a, hash_b, options)


def parse_options():
  """Parse and return the options and positional arguments."""
  parser = argparse.ArgumentParser(
      usage="%(prog)s [options] git_hash_A [git_hash_B]",
      formatter_class=argparse.RawDescriptionHelpFormatter,
      description=textwrap.dedent("""\
        When one hash is given, measures the performance on the specified workloads.
        When two hashes are given, compares their performance. Output is in
        $IMPALA_HOME/perf_results/latest. In the performance_result_*.txt files,
        git_hash_A is referred to as the "Base" result. For example, if you run with
        git_hash_A = aBad1dea... and git_hash_B = 8675309... the
        performance_result_*.txt will say at the top:

          Run Description: "aBad1dea... vs 8675309..."

        The different queries will have their run time statistics in columns
        "Avg(s)", "StdDev(%)", "BaseAvg(s)", "Base StdDev(%)". The first two refer
        to git_hash_B, the second two refer to git_hash_A. The column "Delta(Avg)"
        is negative if git_hash_B is faster and is positive if git_hash_A is faster.

        WARNING: This script will run git checkout. You should not touch the tree
        while the script is running. You should start the script from a clean git
        tree.

        WARNING: When --load is used, this script calls load_data.py which can
        overwrite your TPC-H and TPC-DS data."""))
  parser.add_argument("git_hash_A", metavar="git_hash_A",
                      help="git hash (or ref) to measure; the \"Base\" in comparisons")
  parser.add_argument("git_hash_B", metavar="git_hash_B", nargs="?",
                      help="optional second git hash (or ref) to compare against")
  parser.add_argument("--workloads", default="targeted-perf",
                      help="comma-separated list of workloads. Choices: tpch, "
                      "targeted-perf, tpcds. Default: targeted-perf")
  parser.add_argument("--scale", required=True,
                      help="scale factor for the workloads [required]")
  parser.add_argument("--iterations", type=int, default=30,
                      help="number of times to run each query. Default: 30")
  parser.add_argument("--table_formats", default="parquet/none", help="comma-separated "
                      "list of table formats. Default: parquet/none")
  parser.add_argument("--num_impalads", type=int, default=1,
                      help="number of executor impalads. Default: 1")
  parser.add_argument("--num_coordinators", type=int, default=1,
                      help="number of coordinators. Queries are sent only to the "
                      "coordinators. Default: 1")
  parser.add_argument("--use_exclusive_coordinators", action="store_true",
                      help="coordinators do not execute fragments and are started in "
                      "addition to --num_impalads executors")
  # Less commonly-used options:
  parser.add_argument("--query_names",
                      help="comma-separated list of regular expressions. A query is "
                      "executed if it matches any regular expression in this list")
  parser.add_argument("--load", action="store_true",
                      help="load databases for the chosen workloads")
  parser.add_argument("--start_minicluster", action="store_true",
                      help="start a new Hadoop minicluster")
  parser.add_argument("--ninja", action="store_true",
                      help="use ninja, rather than Make, as the build tool")
  parser.add_argument("--impalad_args", action="append", default=[],
                      help="Additional arguments to pass to each Impalad during startup")
  parser.add_argument("--exec_options",
                      help=("Query exec option string to run workload (formatted as "
                        "'opt1:val1;opt2:val2')"))
  parser.add_argument("--max_profiles", type=int,
                      help="maximum number of runtime profiles to keep per query (the "
                      "first N iterations). Default: no limit")

  options = parser.parse_args()

  if options.num_coordinators < 1:
    parser.error("--num_coordinators must be at least 1")
  if (not options.use_exclusive_coordinators
      and options.num_coordinators > options.num_impalads):
    parser.error("--num_coordinators cannot exceed --num_impalads unless "
                 "--use_exclusive_coordinators is set")

  args = [options.git_hash_A]
  if options.git_hash_B:
    args.append(options.git_hash_B)
  return options, args


def main():
  """A thin wrapper around perf_ab_test that restores git state after."""
  options, args = parse_options()

  os.chdir(IMPALA_HOME)

  if get_git_output(["status", "--porcelain", "--untracked-files=no"]).strip():
    run_git(["status", "--porcelain", "--untracked-files=no"])
    # Something went wrong, let's dump the actual diff to make it easier to
    # track down
    print("#### Working copy is dirty, dumping the diff #####")
    run_git(["--no-pager", "diff"])
    print("#### End of diff #####")
    raise Exception("Working copy is dirty. Consider 'git stash' and try again.")

  # Save the current hash to be able to return to this place in the tree when done
  current_hash = get_git_output(["rev-parse", "--abbrev-ref", "HEAD"]).strip()
  if current_hash == "HEAD":
    current_hash = get_git_hash_for_name("HEAD")

  try:
    workloads = backup_workloads()
    perf_ab_test(options, args)
  finally:
    # discard any changes created by the previous restore_workloads()
    shutil.rmtree("testdata/workloads")
    run_git(["checkout", "--", "testdata/workloads"])
    run_git(["checkout", current_hash])
    restore_workloads(workloads)


if __name__ == "__main__":
  main()
