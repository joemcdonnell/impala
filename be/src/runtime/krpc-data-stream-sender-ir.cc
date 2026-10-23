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

#include "exec/hash-table.inline.h"
#include "exec/partitioned-hash-join-builder.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/krpc-data-stream-sender.h"
#include "runtime/outbound-row-batch.inline.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"

namespace impala {

ScalarExprEvaluator* KrpcDataStreamSender::GetPartitionExprEvaluator(int i) {
  return partition_expr_evals_[i];
}

Status KrpcDataStreamSender::HashAndAddRows(RowBatch* batch) {
  const int num_rows = batch->num_rows();
  const int num_channels = GetNumChannels();
  // Hash / deepcopy in batches of HASH_BATCH_SIZE.
  // This optimization (IMPALA-6461) can be probably removed if deepcopy gets
  // codegend.
  int channel_ids[RowBatch::HASH_BATCH_SIZE];
  int row_idx = 0;
  while (row_idx < num_rows) {
    int row_count = 0;
    FOREACH_ROW_LIMIT(batch, row_idx, RowBatch::HASH_BATCH_SIZE, row_batch_iter) {
      TupleRow* row = row_batch_iter.Get();
      channel_ids[row_count++] = HashRow(row, exchange_hash_seed_) % num_channels;
    }
    row_count = 0;
    FOREACH_ROW_LIMIT(batch, row_idx, RowBatch::HASH_BATCH_SIZE, row_batch_iter) {
      int channel_id = channel_ids[row_count++];
      PartitionRowCollector& collector = partition_row_collectors_[channel_id];
      RETURN_IF_ERROR(collector.AppendRow(row_batch_iter.Get(), row_desc_));
    }
    row_idx += row_count;
  }
  return Status::OK();
}

Status KrpcDataStreamSender::PartitionRowCollector::AppendRow(
    const TupleRow* row, const RowDescriptor* row_desc) {
  DCHECK_LT(num_rows_, row_batch_capacity_);
  num_rows_++;
  RETURN_IF_ERROR(collector_batch_->AppendRow(row, row_desc));
  DCHECK_GT(row_batch_capacity_, 0);
  if (UNLIKELY(
      num_rows_ == row_batch_capacity_ || collector_batch_->ReachedSizeLimit())) {
    // This swaps collector_batch_ with an empty batch.
    RETURN_IF_ERROR(SendCurrentBatch());
  }
  return Status::OK();
}

Status KrpcDataStreamSender::AddRowsToBuildWorkers(RowBatch* batch) {
  const int num_build_workers = build_worker_collectors_.size();
  HashTableCtx::ExprValuesCache* expr_vals_cache = join_build_ht_ctx_->expr_values_cache();
  expr_vals_cache->Reset();
  FOREACH_ROW(batch, 0, batch_iter) {
    TupleRow* row = batch_iter.Get();
    // Use the same hash function and partition as PhjBuilder::ProcessBuildBatch(). It
    // does not add rows with a NULL in an equi-join key to the hash table (unless the
    // join stores NULLs), so they do not have a partition. They are sent to the first
    // worker, which skips them like the serial build does.
    int worker_idx = 0;
    if (join_build_ht_ctx_->EvalAndHashBuild(row)) {
      worker_idx = PhjBuilder::BuildWorkerForPartition(
          PhjBuilder::PartitionIndex(expr_vals_cache->CurExprValuesHash()),
          num_build_workers);
    }
    RETURN_IF_ERROR(build_worker_collectors_[worker_idx].AppendRow(row, row_desc_));
  }
  return Status::OK();
}

Status KrpcDataStreamSender::BuildWorkerRowCollector::AppendRow(
    const TupleRow* row, const RowDescriptor* row_desc) {
  DCHECK_LT(num_rows_, row_batch_capacity_);
  ++num_rows_;
  RETURN_IF_ERROR(collector_batch_->AppendRow(row, row_desc));
  if (UNLIKELY(num_rows_ == row_batch_capacity_ || collector_batch_->ReachedSizeLimit())) {
    RETURN_IF_ERROR(SendCurrentBatch());
  }
  return Status::OK();
}

}
