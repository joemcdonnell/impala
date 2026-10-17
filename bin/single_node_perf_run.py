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
# $IMPALA_HOME/perf_results/latest. A separate report is produced for each combination
# of workload and table format, named performance_result_<workload>_<table_format>.txt
# (with '/' in the table format replaced by '_'). In each report file,
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
#                         number of impalads. Default: 1
#   --query_names=QUERY_NAMES
#                         comma-separated list of regular expressions. A query
#                         is executed if it matches any regular expression in
#                         this list
#   --load                load databases for the chosen workloads
#   --start_minicluster   start a new Hadoop minicluster
#   --ninja               use ninja, rather than Make, as the build tool
#   --exec_options        query exec option string to run workload
#                         (formatted as 'opt1:val1;opt2:val2')

from optparse import OptionParser
from tempfile import mkdtemp

import json
import os
import pipes
import shutil
import subprocess
import sys
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


def start_impala(num_impalads, options):
  configured_call(["{0}/bin/start-impala-cluster.py".format(IMPALA_HOME), "-s",
                   str(num_impalads), "-c", str(num_impalads)]
                  + ["--impalad_args={0}".format(arg) for arg in options.impalad_args])


def get_label(workload, table_format):
  """Returns a string identifying a workload / table format combination."""
  return "{0}_{1}".format(workload, table_format.replace("/", "_"))


def get_result_file(base_dir, git_hash, label):
  """Returns the path of the JSON results for a git hash and workload/table format."""
  return os.path.join(base_dir, "{0}_{1}.json".format(git_hash, label))


def run_workload(base_dir, workload, table_format, options):
  """Runs a single workload with a single table format with the given options.

  Writes the results to a file identified by the git hash, workload and table format.
  """
  git_hash = get_git_hash_for_name("HEAD")
  label = get_label(workload, table_format)

  run_workload = ["{0}/bin/run-workload.py".format(IMPALA_HOME)]

  impalads = ",".join(["localhost:{0}".format(21050 + i)
                       for i in range(0, int(options.num_impalads))])

  run_workload += ["--workload={0}:{1}".format(workload, options.scale),
                   "--impalads={0}".format(impalads),
                   "--results_json_file={0}".format(
                       get_result_file(base_dir, git_hash, label)),
                   "--query_iterations={0}".format(options.iterations),
                   "--table_format={0}".format(table_format),
                   "--plan_first"]

  if options.exec_options:
    run_workload += ["--exec_options={0}".format(options.exec_options)]

  if options.query_names:
    run_workload += ["--query_names={0}".format(options.query_names)]

  configured_call(run_workload)


def run_workloads(base_dir, workloads, table_formats, options):
  """Runs every combination of workload and table format."""
  for workload in workloads:
    for table_format in table_formats:
      run_workload(base_dir, workload, table_format, options)


def report_benchmark_results(file_a, file_b, description, label):
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
                        "performance_result_{0}.txt".format(label))
  with open(result, "w") as f:
    f.write(performance_result)


def compare(base_dir, hash_a, hash_b, label, options):
  """Take the results of two performance runs of one workload / table format and
  compare them."""
  file_a = get_result_file(base_dir, hash_a, label)
  file_b = get_result_file(base_dir, hash_b, label)
  description = "{0} vs {1} ({2})".format(hash_a, hash_b, label)
  report_benchmark_results(file_a, file_b, description, label)

  # Qualify the hashes with the label so different combinations don't collide
  name_a = "{0}_{1}".format(hash_a, label)
  name_b = "{0}_{1}".format(hash_b, label)
  diff_file = "performance_result_profile_diff_{0}.txt".format(label)

  # From the two json files extract the profiles and diff them
  if options.split_profiles:
    generate_profile_files(file_a, name_a, base_dir)
    generate_profile_files(file_b, name_b, base_dir)
    with open(os.path.join(IMPALA_HOME, diff_file), "w") as f:
      # This does not check that the diff command succeeds
      subprocess.run(["diff", "-u", os.path.join(base_dir, name_a + "_profiles"),
        os.path.join(base_dir, name_b + "_profiles")], stdout=f, text=True, env={})
  else:
    generate_profile_file(file_a, name_a, base_dir)
    generate_profile_file(file_b, name_b, base_dir)
    with open(os.path.join(IMPALA_HOME, diff_file), "w") as f:
      # This does not check that the diff command succeeds
      subprocess.run(["diff", "-u", os.path.join(base_dir, name_a + "_profile.txt"),
        os.path.join(base_dir, name_b + "_profile.txt")], stdout=f, text=True, env={})


def generate_profile_file(name, hash, base_dir):
  """Extracts runtime profiles from the JSON file 'name'.

  Writes the runtime profiles back in a simple text file in the same directory.
  """
  with open(name, 'rb') as fid:
    data = json.loads(fid.read().decode("utf-8", "ignore"))
    with open(os.path.join(base_dir, hash + "_profile.txt"), "w+") as out:
      # For each query
      for key in data:
        for iteration in data[key]:
          out.write(iteration["runtime_profile"])
          out.write("\n\n")


def generate_profile_files(name, hash, base_dir):
  """Extracts runtime profiles from the JSON file 'name'.

  Writes the runtime profiles back as separated simple text file in '[hash]_profiles' dir
  in base_dir.
  """
  profile_dir = os.path.join(base_dir, hash + "_profiles")
  if not os.path.exists(profile_dir):
    os.makedirs(profile_dir)
  with open(name, 'rb') as fid:
    data = json.loads(fid.read().decode("utf-8", "ignore"))
    iter_num = {}
    # For each query
    for key in data:
      for iteration in data[key]:
        query_name = iteration["query"]["name"]
        if query_name in iter_num:
          iter_num[query_name] += 1
        else:
          iter_num[query_name] = 1
        curr_iter = iter_num[query_name]

        file_name = "{}_iter{:03d}.txt".format(query_name, curr_iter)
        with open(os.path.join(profile_dir, file_name), "w") as out:
          out.write(iteration["runtime_profile"])


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
  start_impala(options.num_impalads, options)

  workloads = options.workloads.split(",")
  table_formats = options.table_formats.split(",")

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

  run_workloads(temp_dir, workloads, table_formats, options)

  if len(args) > 1 and args[1]:
    hash_b = get_git_hash_for_name(args[1])
    # discard any changes created by the previous restore_workloads()
    shutil.rmtree("testdata/workloads")
    run_git(["checkout", "--", "testdata/workloads"])
    build(hash_b, options)
    restore_workloads(workload_dir)
    start_impala(options.num_impalads, options)
    run_workloads(temp_dir, workloads, table_formats, options)
    for workload in workloads:
      for table_format in table_formats:
        compare(temp_dir, hash_a, hash_b, get_label(workload, table_format), options)


def parse_options():
  """Parse and return the options and positional arguments."""
  parser = OptionParser()
  parser.add_option("--workloads", default="targeted-perf",
                    help="comma-separated list of workloads. Choices: tpch, "
                    "targeted-perf, tpcds. Default: targeted-perf")
  parser.add_option("--scale", help="scale factor for the workloads [required]")
  parser.add_option("--iterations", default=30, help="number of times to run each query")
  parser.add_option("--table_formats", default="parquet/none", help="comma-separated "
                    "list of table formats. Default: parquet/none")
  parser.add_option("--num_impalads", default=1, help="number of impalads. Default: 1")
  # Less commonly-used options:
  parser.add_option("--query_names",
                    help="comma-separated list of regular expressions. A query is "
                    "executed if it matches any regular expression in this list")
  parser.add_option("--load", action="store_true",
                    help="load databases for the chosen workloads")
  parser.add_option("--start_minicluster", action="store_true",
                    help="start a new Hadoop minicluster")
  parser.add_option("--ninja", action="store_true",
                    help="use ninja, rather than Make, as the build tool")
  parser.add_option("--impalad_args", dest="impalad_args", action="append", type="string",
                    default=[],
                    help="Additional arguments to pass to each Impalad during startup")
  parser.add_option("--split_profiles", action="store_true", dest="split_profiles",
                    default=True, help=("If specified, query profiles will be generated "
                      "as separate files"))
  parser.add_option("--no_split_profiles", action="store_false", dest="split_profiles",
                    help=("If specified, query profiles will be generated as a "
                      "single-combined file"))
  parser.add_option("--exec_options", dest="exec_options",
                    help=("Query exec option string to run workload (formatted as "
                      "'opt1:val1;opt2:val2')"))

  parser.set_usage(textwrap.dedent("""
    single_node_perf_run.py [options] git_hash_A [git_hash_B]

    When one hash is given, measures the performance on the specified workloads.
    When two hashes are given, compares their performance. Output is in
    $IMPALA_HOME/perf_results/latest. In the performance_result.txt file,
    git_hash_A is referred to as the "Base" result. For example, if you run with
    git_hash_A = aBad1dea... and git_hash_B = 8675309... the
    performance_result.txt will say at the top:

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

  options, args = parser.parse_args()

  if not 1 <= len(args) <= 2:
    parser.print_usage(sys.stderr)
    raise Exception("Invalid arguments: either 1 or 2 Git hashes allowed")

  if not options.scale:
    parser.print_help(sys.stderr)
    raise Exception("--scale is required")

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
