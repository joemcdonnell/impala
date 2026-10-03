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
# $IMPALA_HOME/perf_results/latest. In the performance_result.txt file,
# git_hash_A is referred to as the "Base" result. For example, if you run with
# git_hash_A = aBad1dea... and git_hash_B = 8675309... the
# performance_result.txt will say at the top:
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
import platform
import shutil
import subprocess
import sys
import textwrap

from pathlib import Path
from tests.common.test_dimensions import TableFormatInfo

IMPALA_HOME = os.environ["IMPALA_HOME"]
IMPALA_PERF_RESULTS = os.path.join(IMPALA_HOME, "perf_results")


# We are running in a base cgroup (V2 only) with the following requirements:
# 1. It has cpusets enabled (i.e. cgroup.controllers contains cpuset)
# 2. It has cpuset.cpus and cpuset.mems set
# 3. We have permissions to manipulate it and create subgroups.
# For Cgroups V2, the usual way to do this is to run this inside a systemd service
# with Delegate, AllowedCPUs, and AllowedMemoryNodes set.
#
# We do not have permissions outside of our base cgroup, so this should never
# access anything outside it. When running via systemd, there won't be subgroups
# that need to be cleaned up, but this supports cleaning them up anyway.
class CgroupHandler:
  def __init__(self, base_cgroup):
    # For this to work properly, the base_cgroup needs to be a relative path. Check that
    # it does not start with /
    if base_cgroup[0] == "/":
      raise Exception("Base cgroup must be a relative path: {0}".format(base_cgroup))
    self.base_cgroup = Path("/sys/fs/cgroup") / base_cgroup
    # Check preconditions for using this cgroup
    self.validate_base_cgroup()
    # When running via systemd, there won't be leftover subgroups, but this cleans them
    # up anyway.
    self.cleanup_subgroups()

  def validate_base_cgroup(self):
    # 0. Check for existence of the cgroup's directory
    if not self.base_cgroup.exists():
      raise Exception("No such cgroup: {0}".format(self.base_cgroup))
    if not self.base_cgroup.is_dir():
      raise Exception("Cgroup path is not a directory: {0}".format(self.base_cgroup))

    # 1. The cgroup must support cpusets
    with open(self.base_cgroup / "cgroup.controllers") as f:
      contents = f.read()
      if "cpuset" not in contents:
        raise Exception("cpuset support not present in cgroup.controllers. "
                        "Contents: {0}".format(contents))

    # 2. cpuset.cpus and cpuset.mems must be set (this means that the systemd service
    # needs to specify AllowedCPUs and AllowedMemoryNodes)
    with open(self.base_cgroup / "cpuset.cpus") as f:
      contents = f.read().strip()
      if len(contents) == 0:
        raise Exception("cpuset.cpus must be set on the base cgroup")
      self.base_cpus = contents

    with open(self.base_cgroup / "cpuset.mems") as f:
      contents = f.read().strip()
      if len(contents) == 0:
        raise Exception("cpuset.mems must be set on the base cgroup")
      self.base_mems = contents

  def cleanup_subgroups(self):
    self.cleanup_subgroups_helper(Path(self.base_cgroup), False)

  @staticmethod
  def cleanup_subgroups_helper(cgroup_path, delete_this_path):
    # This is a recursive function to clean up any subgroups (which can be nested).
    # This takes in a pathlib Path.
    subdirs = [x for x in cgroup_path.iterdir() if x.is_dir()]
    for subdir in subdirs:
      # When recursing, we want to actually remove the dir
      CgroupHandler.cleanup_subgroups_helper(subdir, True)
    # All subgroups are gone. We can remove this dir if desired.
    if delete_this_path:
      cgroup_path.rmdir()

  def enable_cpusets(self):
    # Adding a feature is done by writing +feature to cgroup.subtree_control. This needs
    # to happen after we've created subgroups and moved the main process out of the base
    # cgroup.
    with open(self.base_cgroup / "cgroup.subtree_control", "w") as f:
      f.write("+cpuset\n")

  def get_base_cgroup_cpus(self):
    return CgroupHandler.comma_dash_string_to_integer_list(self.base_cpus)

  @staticmethod
  def comma_dash_string_to_integer_list(string):
    # The comma dash string is like "0-3,7-8" and we want to convert that to a list of
    # integers (e.g. [0,1,2,3,7,8]). We assume that the ranges are not overlapping.
    int_list = []
    for s in string.split(","):
      if "-" in s:
        start, end = s.split("-")
        if end <= start:
          raise Exception("Invalid range {0}".format(s))
        int_list.extend(range(int(start), int(end) + 1))
      else:
        if s != "":
          int_list.append(int(s))
    return int_list

  @staticmethod
  def integer_list_to_comma_dash_string(int_list):
    # Given an integer list, convert it to a comma dash string. This is the reverse of
    # comma_dash_string_to_integer_list, so [0,1,2,3,7,8] => "0-3,7-8"
    if len(int_list) == 0:
      return ""
    sorted_int_list = sorted(int_list)
    string_pieces = []
    start_range = 0
    for i in range(1, len(sorted_int_list)):
      # If the integers are contiguous, continue with this start_range
      if sorted_int_list[i - 1] + 1 == sorted_int_list[i]:
        continue
      else:
        # We are at the end of a contiguous range
        # If there is only one element, emit it alone, otherwise emit the x-y pair
        if start_range == i - 1:
          string_pieces.append(str(sorted_int_list[start_range]))
        else:
          string_pieces.append("{0}-{1}".format(sorted_int_list[start_range],
              sorted_int_list[i - 1]))
        # Starting a new contiguous range
        start_range = i

    # We are at the end of the list, so that's the end of a contiguous range
    # Emit what is left.
    if start_range == len(sorted_int_list) - 1:
      string_pieces.append(str(sorted_int_list[start_range]))
    else:
      string_pieces.append("{0}-{1}".format(sorted_int_list[start_range],
          sorted_int_list[-1]))

    return ",".join(string_pieces)

  def create_subgroup(self, subgroup_name):
    # Simply create subdirectory
    subgroup_path = self.base_cgroup / subgroup_name
    subgroup_path.mkdir()

  def set_cpu_list(self, subgroup_name, cpu_id_list):
    subgroup_path = self.base_cgroup / subgroup_name
    if not subgroup_path.exists():
      raise Exception("No such cgroup: {0}".format(subgroup_path))
    # Validate that the cpu_list is a subset of the CPUs assigned to the base
    # cgroup
    subgroup_cpu_id_set = set(cpu_id_list)
    base_cpu_id_set = set(self.get_base_cgroup_cpus())
    invalid_cpus = subgroup_cpu_id_set.difference(base_cpu_id_set)
    if len(invalid_cpus) != 0:
      raise Exception("Requested CPUs not in the base cgroup: {0}".format(invalid_cpus))

    cpu_id_string = CgroupHandler.integer_list_to_comma_dash_string(cpu_id_list)

    # Write to the cpuset.cpus file
    with open(subgroup_path / "cpuset.cpus", "w") as f:
      f.write(cpu_id_string)
    # Write to the cpuset.mems file (just use the base cgroup's setting)
    with open(subgroup_path / "cpuset.mems", "w") as f:
      f.write(self.base_mems)

  def put_pid_in_cgroup(self, pid, subgroup_name):
    # Verify the cgroup exists
    subgroup_path = self.base_cgroup / subgroup_name
    if not subgroup_path.exists():
      raise Exception("No such cgroup: {0}".format(subgroup_path))
    if not subgroup_path.is_dir():
      raise Exception("Cgroup path is not a directory: {0}".format(subgroup_path))

    # Write our pid to the tasks file
    with open(subgroup_path / "cgroup.procs", "w") as f:
      f.write(str(pid))


def configured_call(cmd):
  """Call a command in a shell with config-impala.sh."""
  if type(cmd) is list:
    cmd = " ".join([pipes.quote(arg) for arg in cmd])
  cmd = "source {0}/bin/impala-config.sh && {1}".format(IMPALA_HOME, cmd)
  # Set env={} to use a clean environment. Existing environment variables can cause
  # complicated interactions. For example, this script runs with impala-python3,
  # which sets LD_LIBRARY_PATH to use the toolchain libstdc++. On newer OSes, this
  # can cause issues for system binaries that need to use a newer libstdc++.
  pathonlyenv = {}
  pathonlyenv["PATH"] = os.environ["PATH"]
  return subprocess.check_call(["bash", "-c", cmd], env=pathonlyenv)


def run_git(args):
  """Runs git without capturing output (stdout passes through to stdout)"""
  subprocess.check_call(["git"] + args, text=True, env={})


def get_git_output(args):
  """Runs git, capturing the output and returning it"""
  return subprocess.check_output(["git"] + args, text=True, env={})


def load_data(workload, table_formats, scale, options):
  """Loads a database with a particular scale factor."""
  if options.custom_dataload_script:
    dataload_script = options.custom_dataload_script
    # This doesn't add text/none to the list when using the custom dataload
    # script, because it may not be necessary and the custom dataload script
    # needs to handle any dependencies itself. Arguably, bin/load-data.py
    # should do the same.
    all_formats = table_formats
  else:
    dataload_script = "{0}/bin/load-data.py".format(IMPALA_HOME)
    all_formats = ("text/none," + table_formats if "text/none" not in table_formats
                   else table_formats)

  configured_call([dataload_script, "--workloads", workload,
                   "--scale_factor", str(scale), "--table_formats", all_formats])
  # We could require the custom dataload script to do this itself, but for now
  # it seems basically fine to compute stats.
  for table_format in table_formats.split(","):
    suffix = TableFormatInfo.create_from_string(None, table_format).db_suffix()
    db_name = workload + scale + suffix
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


def start_minicluster(cgroup_handler):
  # We want the minicluster to run in the "other" group. Switch into the "other" group
  # to start the minicluster, then switch back to the "admin" group.
  put_pid_in_cgroup(cgroup_handler, os.getpid(), "other")
  configured_call(["{0}/bin/create-test-configuration.sh".format(IMPALA_HOME)])
  configured_call(["{0}/testdata/bin/run-all.sh".format(IMPALA_HOME)])
  put_pid_in_cgroup(cgroup_handler, os.getpid(), "admin")


def stop_minicluster():
  configured_call(["{0}/testdata/bin/kill-all.sh".format(IMPALA_HOME)])


def start_impala(num_impalads, options, cgroup_handler):
  start_impala_cluster_cmd = ["{0}/bin/start-impala-cluster.py".format(IMPALA_HOME)]
  start_impala_cluster_cmd.extend(["-s", str(num_impalads), "-c", str(num_impalads)])
  start_impala_cluster_cmd.extend(options.start_impala_cluster_args)
  for arg in options.impalad_args:
    start_impala_cluster_cmd.append("--impalad_args={0}".format(arg))
  # We want the non-impalad daemons like statestored/catalogd in the "other" group,
  # so switch in before starting, then switch back to "admin".
  put_pid_in_cgroup(cgroup_handler, os.getpid(), "other")
  configured_call(start_impala_cluster_cmd)
  put_pid_in_cgroup(cgroup_handler, os.getpid(), "admin")
  if cgroup_handler:
    # Get the Impalad pids
    output = subprocess.check_output(["pgrep", "impalad"], text=True)
    impalad_pids = sorted([int(x) for x in output.strip().split("\n")])
    num_found_impalads = len(impalad_pids)
    if num_found_impalads != num_impalads:
      raise Exception("Expected {0} impalads but found {1}".format(
          num_impalads, num_found_impalads))
    # The impalad_pids are sorted, and we are putting each pid in its own cgroup.
    # The first cgroup goes to impalad1, the second to impalad2, and so on.
    for impalad_idx, pid in enumerate(impalad_pids):
      put_pid_in_cgroup(cgroup_handler, pid, "impalad{0}".format(impalad_idx + 1))


def stop_impala():
  configured_call(["{0}/bin/start-impala-cluster.py".format(IMPALA_HOME), "--kill"])


def run_workload(base_dir, workloads, options):
  """Runs workload with the given options.

  Returns the git hash of the current revision to identify the output file.
  """
  git_hash = get_git_hash_for_name("HEAD")

  run_workload = ["{0}/bin/run-workload.py".format(IMPALA_HOME)]

  impalads = ",".join(["localhost:{0}".format(21050 + i)
                       for i in range(0, int(options.num_impalads))])

  run_workload += ["--workloads={0}".format(workloads),
                   "--impalads={0}".format(impalads),
                   "--results_json_file={0}/{1}.json".format(base_dir, git_hash),
                   "--query_iterations={0}".format(options.iterations),
                   "--table_formats={0}".format(options.table_formats),
                   "--plan_first"]

  if options.exec_options:
    run_workload += ["--exec_options={0}".format(options.exec_options)]

  if options.query_names:
    run_workload += ["--query_names={0}".format(options.query_names)]

  configured_call(run_workload)


def report_benchmark_results(file_a, file_b, description):
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
  result = os.path.join(IMPALA_PERF_RESULTS, "latest", "performance_result.txt")
  with open(result, "w") as f:
    f.write(performance_result)


def compare(base_dir, hash_a, hash_b):
  """Take the results of two performance runs and compare them."""
  file_a = os.path.join(base_dir, hash_a + ".json")
  file_b = os.path.join(base_dir, hash_b + ".json")
  description = "{0} vs {1}".format(hash_a, hash_b)
  report_benchmark_results(file_a, file_b, description)

  # From the two json files extract the profiles and diff them
  generate_profile_files(file_a, hash_a, base_dir)
  generate_profile_files(file_b, hash_b, base_dir)
  with open(os.path.join(IMPALA_HOME, "performance_result_profile_diff.txt"), "w") as f:
    # This does not check that the diff command succeeds
    subprocess.run(["diff", "-u", os.path.join(base_dir, hash_a + "_profiles"),
      os.path.join(base_dir, hash_b + "_profiles")], stdout=f, text=True, env={})


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


# Read /proc/cpuinfo and produce a map from the core id to the cpu ids.
# This is important for hyperthreaded systems, as it gives us information about
# which cpu ids are hyperthreads on the same core.
def get_core_to_cpu_id_map(allowed_cpu_ids):
  assert platform.processor() in ["x86_64", "aarch64"]
  core_to_cpu_id_map = {}

  def add_core_to_cpu_id_entry(core_id, cpu_id):
    if cpu_id not in allowed_cpu_ids:
      return
    if core_id not in core_to_cpu_id_map:
      core_to_cpu_id_map[core_id] = [cpu_id]
    else:
      core_to_cpu_id_map[core_id].append(cpu_id)

  with open("/proc/cpuinfo") as f:
    cur_cpu_id = -1
    for line in f:
      # The behavior of /proc/cpuinfo is platform specific, so this has separate logic
      # for x86_64 and ARM.
      if platform.processor() == "x86_64":
        # On x86_64, /proc/cpuinfo starts each section with "processor", and the
        # "core id" will come later in that same section. So, we can associate each
        # processor to the core id that follows, and this provides hyperthreading
        # information.
        if line.startswith("processor"):
          cur_cpu_id = int(line.split(":")[1].strip())
        elif line.startswith("core id"):
          assert platform.processor() == "x86_64"
          core_id = int(line.split(":")[1].strip())
          add_core_to_cpu_id_entry(core_id, cur_cpu_id)
      else:
        # ARM is not hyperthreaded and there is no "core id" field, so treat each cpu id
        # as the core id
        assert platform.processor() == "aarch64"
        if line.startswith("processor"):
          cur_cpu_id = int(line.split(":")[1].strip())
          add_core_to_cpu_id_entry(cur_cpu_id, cur_cpu_id)

  return core_to_cpu_id_map


def create_cgroups(options, cgroup_handler):
  # This sets up num_impalads+2 cgroups. There is a single "admin" group with access
  # to all the CPUs. Then, there is one per impalad and a catch-all
  # "other" group for everything else (like the minicluster, etc).
  #
  # Things need to go in this order:
  # 1. Create subgroup(s) and move the main process into a subgroup to avoid
  #    triggering Cgroup V2's no-interior-processes rule
  # 2. Turn on cpusets for the subgroups
  # 3. Set the cpus for each of the subgroups
  num_impalads = options.num_impalads

  # 1. Create "admin" subgroup and move into it, then create subgroups for Impalads
  cgroup_handler.create_subgroup("admin")
  cgroup_handler.put_pid_in_cgroup(os.getpid(), "admin")
  cgroup_handler.create_subgroup("other")
  for impalad_idx in range(num_impalads):
    cgroup_handler.create_subgroup("impalad{0}".format(impalad_idx + 1))

  # 2. Enable cpusets
  cgroup_handler.enable_cpusets()

  # 3. Set the CPUs for each cgroup. The "admin" cgroup can use all CPUs. Each impalad
  # gets cpus_per_impalad CPUs assigned, starting from the 0th CPU. The "other" cgroup
  # is for everything else, and it gets any remaining CPUs.
  cpus_per_impalad = options.cpus_per_impalad
  cpu_list = cgroup_handler.get_base_cgroup_cpus()

  # Set "admin" to use all CPUs
  print("Using all CPUs for admin: {0}".format(cpu_list))
  cgroup_handler.set_cpu_list("admin", cpu_list)

  core_to_cpu_id_map = get_core_to_cpu_id_map(cpu_list)
  core_list = list(core_to_cpu_id_map.keys())
  if len(core_list) == len(cpu_list):
    # No hyperthreading
    cores_per_impalad = cpus_per_impalad
  else:
    # This should be hyperthreaded. Let's verify that every core has two threads
    for core_id, cpu_id_list in core_to_cpu_id_map.items():
      if len(cpu_id_list) != 2:
        raise Exception("Core id {0} has {1} threads, expected 2. Full info: {2}".format(
            core_id, len(cpu_id_list), core_to_cpu_id_map))
    if cpus_per_impalad % 2 != 0:
      raise Exception("cpus_per_impalad is not even and this is a hyperthreaded machine.")
    cores_per_impalad = int(cpus_per_impalad / 2)

  # There needs to be at least one extra CPU once we give each Impalad the specified
  # number of CPUs
  requested_cpus = num_impalads * cpus_per_impalad + 1
  if len(cpu_list) < requested_cpus:
    raise Exception("Provided cpuset cgroup cannot satisfy request for {0} cores".format(
        requested_cpus))

  # Create one cgroup per impalad and assign cpus_per_impalad to it.
  # We simply slice up the cpu_list.
  for impalad_idx in range(num_impalads):
    start_core_idx = impalad_idx * cores_per_impalad
    end_core_idx = (impalad_idx + 1) * cores_per_impalad
    core_slice = core_list[start_core_idx:end_core_idx]
    cpu_slice = []
    for core_id in core_slice:
      if cores_per_impalad != cpus_per_impalad and options.disable_hyperthreading:
        # Only use the first thread of each core
        cpu_slice.append(core_to_cpu_id_map[core_id][0])
      else:
        cpu_slice.extend(core_to_cpu_id_map[core_id])
    if options.disable_hyperthreading:
      assert len(cpu_slice) == cores_per_impalad
    else:
      assert len(cpu_slice) == cpus_per_impalad
    print("Handing out cpus {0} to Impala {1}".format(cpu_slice, impalad_idx + 1))
    cgroup_handler.set_cpu_list("impalad{0}".format(impalad_idx + 1), cpu_slice)

  # Now, we create a other cgroup for all the non-impalad processes
  # This gets all remaining CPUs not used by the impalads
  remaining_cores = core_list[num_impalads * cores_per_impalad:]
  remaining_cpus = []
  for core_id in remaining_cores:
    remaining_cpus.extend(core_to_cpu_id_map[core_id])
  print("Remaining cpus for other: {0}".format(remaining_cpus))
  cgroup_handler.set_cpu_list("other", remaining_cpus)


def put_pid_in_cgroup(cgroup_handler, pid, cgroup):
  if not cgroup_handler:
    return
  cgroup_handler.put_pid_in_cgroup(pid, cgroup)


def perf_ab_test(options, args):
  """Does the main work: build, run tests, compare."""
  hash_a = get_git_hash_for_name(args[0])

  cgroup_handler = None
  # Cgroups support relies on the caller invoking this script inside a top level
  # cgroup with appropriate permissions to create subgroups with cpusets. This is
  # often done by running this script in a systemd service with Delegate=true.
  if options.use_cgroup_cpusets:
    # This process's current cgroup is the base cgroup, read it from /proc/self/cgroup
    with open("/proc/self/cgroup") as f:
      contents = f.read()
      # Entry is like 0::/user.slice/user-1000.slice/session-2.scope
      # Exract out the last entry (i.e. "/user.slice/user-1000.slice/session-2.scope")
      base_cgroup = contents.split(":")[2].strip()
      # Remove the leading / so it doesn't get treated like an absolute path
      # (i.e. "user.slice/user-1000.slice/session-2.scope")
      base_cgroup = base_cgroup[1:]
    cgroup_handler = CgroupHandler(base_cgroup)
    # Creating the cgroups also moves this process into the "other" subgroup for the
    # duration.
    create_cgroups(options, cgroup_handler)

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
    start_minicluster(cgroup_handler)
  start_impala(options.num_impalads, options, cgroup_handler)

  workloads = options.workloads.split(",")

  if options.load or options.custom_dataload_script:
    WORKLOAD_TO_DATASET = {
      "tpch": "tpch",
      "tpcds": "tpcds",
      "targeted-perf": "tpch",
      "tpcds-unmodified": "tpcds-unmodified",
      "tpcds_partitioned": "tpcds_partitioned"
    }
    datasets = [WORKLOAD_TO_DATASET[workload] for workload in workloads]
    if "tpcds_partitioned" in datasets and "tpcds" not in datasets and \
       not options.custom_dataload_script:
      # "tpcds_partitioned" require the text "tpcds" database.
      load_data("tpcds", "text/none", options.scale, options)
    for dataset in datasets:
      load_data(dataset, options.table_formats, options.scale, options)

  workloads = ",".join(["{0}:{1}".format(workload, options.scale)
                        for workload in workloads])

  # Restart impala after loading data
  stop_impala()
  start_impala(options.num_impalads, options, cgroup_handler)

  run_workload(temp_dir, workloads, options)

  if len(args) > 1 and args[1]:
    hash_b = get_git_hash_for_name(args[1])
    # discard any changes created by the previous restore_workloads()
    shutil.rmtree("testdata/workloads")
    run_git(["checkout", "--", "testdata/workloads"])
    build(hash_b, options)
    restore_workloads(workload_dir)
    start_impala(options.num_impalads, options, cgroup_handler)
    run_workload(temp_dir, workloads, options)
    compare(temp_dir, hash_a, hash_b)

  stop_impala()
  # If we started the minicluster, shut it off at the end
  if options.start_minicluster:
    stop_minicluster()
  # At this point, we have stopped all processes except the runner script.
  # We could move ourself to the parent cgroup and cleanup the subgroups.
  # When running via systemd, this is not needed as systemd will clean up
  # the whole tree when this exits. Let's avoid the complication and skip
  # cleanup for now.


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
  parser.add_option("--num_impalads", default=1, type="int",
                    help="number of impalads. Default: 1")
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
  parser.add_option("--exec_options", dest="exec_options",
                    help=("Query exec option string to run workload (formatted as "
                      "'opt1:val1;opt2:val2')"))
  parser.add_option("--use_cgroup_cpusets", action="store_true",
                    dest="use_cgroup_cpusets", help=("Use cgroup cpusets to put the "
                      "impalads and minicluster on separate cores. This requires "
                      "invoking this script via a systemd service."))
  parser.add_option("--cpus_per_impalad", dest="cpus_per_impalad", type="int", default=1,
                    help="The number of cpu cores per Impalad when using cgroup cpusets")
  parser.add_option("--disable_hyperthreading", action="store_true",
                    dest="disable_hyperthreading", help=("Map Impalads to only one of "
                      "the two hyperthreads when using cgroup cpusets. This does not do "
                      "anything on non-hyperthreaded systems."))
  parser.add_option("--custom_dataload_script", dest="custom_dataload_script",
                    help=("Custom dataload script to use rather than bin/load-data.py. "
                      "Called with the same arguments as bin/load-data.py "
                      "(workloads,scale_factor,table_formats specified as "
                      "--key value commandline arguments)."))
  parser.add_option("--start_impala_cluster_args", dest="start_impala_cluster_args",
                    default=[], action="append", type="string",
                    help=("Additional arguments to pass to bin/start-impala-cluster.py. "
                          "--impalad_args takes precedence and can override this."))

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
