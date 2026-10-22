// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/partitioned-hash-join-builder.h"

#include <algorithm>
#include <cstdint>
#include <limits>

#include "testutil/gtest-util.h"

namespace impala {

// The sender of a parallel join build and the build workers must agree on the partition
// of a row and on the worker that owns the partition.
TEST(PhjBuilderTest, PartitionIndex) {
  EXPECT_EQ(0, PhjBuilder::PartitionIndex(0));
  EXPECT_EQ(PhjBuilder::PARTITION_FANOUT - 1,
      PhjBuilder::PartitionIndex(std::numeric_limits<uint32_t>::max()));
  // The partition is selected by the highest bits of the hash.
  for (uint32_t partition = 0; partition < PhjBuilder::PARTITION_FANOUT; ++partition) {
    uint32_t first = partition << (32 - PhjBuilder::NUM_PARTITIONING_BITS);
    uint32_t last = first | ((1U << (32 - PhjBuilder::NUM_PARTITIONING_BITS)) - 1);
    EXPECT_EQ(partition, PhjBuilder::PartitionIndex(first));
    EXPECT_EQ(partition, PhjBuilder::PartitionIndex(last));
  }
  // Copies, to avoid binding a reference to the static constants.
  const int num_bits = PhjBuilder::NUM_PARTITIONING_BITS;
  const int fanout = PhjBuilder::PARTITION_FANOUT;
  EXPECT_EQ(1 << num_bits, fanout);
}

// Every partition is owned by exactly one worker, for any number of workers.
TEST(PhjBuilderTest, OwnedPartitionsMask) {
  const uint32_t all_partitions = (1U << PhjBuilder::PARTITION_FANOUT) - 1;
  for (int num_workers = 1; num_workers <= PhjBuilder::PARTITION_FANOUT; ++num_workers) {
    uint32_t union_mask = 0;
    for (int worker = 0; worker < num_workers; ++worker) {
      uint32_t mask = PhjBuilder::OwnedPartitionsMask(num_workers, worker);
      // The workers' partitions are disjoint, and every worker owns at least one.
      EXPECT_EQ(0, union_mask & mask) << num_workers << " workers, worker " << worker;
      EXPECT_NE(0, mask) << num_workers << " workers, worker " << worker;
      union_mask |= mask;
      for (uint32_t partition = 0; partition < PhjBuilder::PARTITION_FANOUT;
           ++partition) {
        // The mask and the sender's routing function agree.
        EXPECT_EQ(PhjBuilder::BuildWorkerForPartition(partition, num_workers) == worker,
            ((mask >> partition) & 1) != 0)
            << num_workers << " workers, partition " << partition;
      }
    }
    EXPECT_EQ(all_partitions, union_mask) << num_workers << " workers";
  }
  // A single worker owns all the partitions.
  EXPECT_EQ(all_partitions, PhjBuilder::OwnedPartitionsMask(1, 0));
}

// The partitions are spread evenly over the workers.
TEST(PhjBuilderTest, PartitionsPerWorkerBalanced) {
  for (int num_workers = 1; num_workers <= PhjBuilder::PARTITION_FANOUT; ++num_workers) {
    int min_owned = PhjBuilder::PARTITION_FANOUT;
    int max_owned = 0;
    for (int worker = 0; worker < num_workers; ++worker) {
      int owned =
          __builtin_popcount(PhjBuilder::OwnedPartitionsMask(num_workers, worker));
      min_owned = std::min(min_owned, owned);
      max_owned = std::max(max_owned, owned);
    }
    EXPECT_LE(max_owned - min_owned, 1) << num_workers << " workers";
  }
}

}
