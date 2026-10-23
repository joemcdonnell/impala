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

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.analysis.BinaryPredicate;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.JoinOperator;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.planner.RuntimeFilterGenerator.RuntimeFilter;
import org.apache.impala.thrift.TDataSink;
import org.apache.impala.thrift.TDataSinkType;
import org.apache.impala.thrift.TExplainLevel;
import org.apache.impala.thrift.TJoinBuildSink;
import org.apache.impala.thrift.TQueryOptions;

import com.google.common.base.Preconditions;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

/**
 * Sink to materialize the build side of a join.
 */
public class JoinBuildSink extends DataSink implements SpillableOperator {
  // id of join's build-side table assigned during planning
  private final JoinTableId joinTableId_;

  // Reference to the join node that consumes the build side.
  private final JoinNode joinNode_;

  private final List<Expr> buildExprs_ = new ArrayList<>();

  private final List<RuntimeFilter> runtimeFilters_ = new ArrayList<>();

  // The number of build worker fragment instances per host. 1 unless the join build is
  // done by several workers per host, see computeNumBuildWorkers().
  private final int numBuildWorkers_;

  // The maximum number of build workers per host. Every worker owns at least one of the
  // join builder's partitions. Must not be larger than PhjBuilder::PARTITION_FANOUT.
  public static final int MAX_BUILD_WORKERS = 16;

  /**
   * Returns the number of build worker fragment instances per host to use for the build
   * of 'join', which is 1 if each host has a single build instance. Several workers are
   * only used for the shared build of a broadcast hash join, if requested with the
   * PARALLEL_BROADCAST_JOIN_BUILD query option. Null-aware anti joins need a single
   * builder, and compute_processing_cost does not support it yet. The scheduler creates
   * the instances based on this decision, which is sent in TJoinBuildSink.
   */
  public static int computeNumBuildWorkers(JoinNode join, TQueryOptions queryOptions) {
    int requested = queryOptions.getParallel_broadcast_join_build();
    if (requested <= 1 || queryOptions.isCompute_processing_cost()) return 1;
    if (!(join instanceof HashJoinNode) || !join.canShareBuild()) return 1;
    if (join.getJoinOp() == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) return 1;
    return Math.min(requested, MAX_BUILD_WORKERS);
  }

  /**
   * Creates sink for build side of 'joinNode' (extracts buildExprs_ from joinNode).
   */
  public JoinBuildSink(JoinTableId joinTableId, JoinNode joinNode) {
    this(joinTableId, joinNode, 1);
  }

  /**
   * Creates sink for build side of 'joinNode' with 'numBuildWorkers' build worker
   * instances per host, see computeNumBuildWorkers().
   */
  public JoinBuildSink(JoinTableId joinTableId, JoinNode joinNode, int numBuildWorkers) {
    Preconditions.checkState(joinTableId.isValid());
    Preconditions.checkState(numBuildWorkers >= 1);
    joinTableId_ = joinTableId;
    joinNode_ = joinNode;
    numBuildWorkers_ = numBuildWorkers;
    if (numBuildWorkers > 1) {
      Preconditions.checkState(joinNode instanceof HashJoinNode && joinNode.canShareBuild());
      ((HashJoinNode) joinNode).setNumBuildWorkers(numBuildWorkers);
    }
    Preconditions.checkNotNull(joinNode);
    Preconditions.checkState(joinNode instanceof JoinNode);
    if (joinNode instanceof HashJoinNode) {
      for (Expr eqJoinConjunct: joinNode.getEqJoinConjuncts()) {
        BinaryPredicate p = (BinaryPredicate) eqJoinConjunct;
        // by convention the build exprs are the rhs of the join conjuncts
        buildExprs_.add(p.getChild(1).clone());
      }
    }
    runtimeFilters_.addAll(joinNode.getRuntimeFilters());
  }

  public JoinTableId getJoinTableId() { return joinTableId_; }
  @Override
  public List<RuntimeFilter> getRuntimeFilters() { return runtimeFilters_; }

  @Override
  protected void toThriftImpl(TDataSink tsink) {
    TJoinBuildSink tBuildSink = new TJoinBuildSink();
    tBuildSink.setDest_node_id(joinNode_.getId().asInt());
    tBuildSink.setJoin_op(joinNode_.getJoinOp().toThrift());
    if (joinNode_ instanceof HashJoinNode ||
        joinNode_ instanceof IcebergDeleteJoinNode) {
      tBuildSink.setEq_join_conjuncts(
          joinNode_.getThriftEquiJoinConjuncts(new ThriftSerializationCtx()));
      tBuildSink.setHash_seed(joinNode_.getFragment().getHashSeed());
    }
    for (RuntimeFilter filter : runtimeFilters_) {
      tBuildSink.addToRuntime_filters(
          filter.toThrift(new ThriftSerializationCtx(), null));
    }
    tBuildSink.setShare_build(joinNode_.canShareBuild());
    if (numBuildWorkers_ > 1) tBuildSink.setNum_build_workers(numBuildWorkers_);
    tsink.setJoin_build_sink(tBuildSink);
  }

  @Override
  protected TDataSinkType getSinkType() {
    if (joinNode_ instanceof HashJoinNode) {
      return TDataSinkType.HASH_JOIN_BUILDER;
    } else if (joinNode_ instanceof NestedLoopJoinNode) {
      return TDataSinkType.NESTED_LOOP_JOIN_BUILDER;
    } else {
      Preconditions.checkState(joinNode_ instanceof IcebergDeleteJoinNode);
      return TDataSinkType.ICEBERG_DELETE_BUILDER;
    }
  }

  @Override
  public void appendSinkExplainString(String prefix, String detailPrefix,
      TQueryOptions queryOptions, TExplainLevel detailLevel, StringBuilder output) {
    output.append(String.format("%s%s\n", prefix, "JOIN BUILD"));
    if (detailLevel.ordinal() > TExplainLevel.MINIMAL.ordinal()) {
      output.append(
          detailPrefix + "join-table-id=" + joinTableId_.toString()
            + " plan-id=" + fragment_.getPlanId().toString()
            + " cohort-id=" + fragment_.getCohortId().toString() + "\n");
      if (numBuildWorkers_ > 1) {
        output.append(detailPrefix + "parallel build workers per host: "
            + numBuildWorkers_ + "\n");
      }
      if (!buildExprs_.isEmpty()) {
        output.append(detailPrefix + "build expressions: ")
            .append(Expr.toSql(buildExprs_, DEFAULT) + "\n");
      }
      if (!runtimeFilters_.isEmpty()) {
        output.append(detailPrefix + "runtime filters: ");
        output.append(PlanNode.getRuntimeFilterExplainString(
            runtimeFilters_, true, joinNode_.getId(), detailLevel));
      }
    }
  }

  /**
   * Return an estimate of the number of nodes the fragment with this sink will run
   * on. This is based on the number of nodes of the join node, since they are
   * co-located.
   */
  public int getNumNodes() {
    return joinNode_.getFragment().getNumNodes();
  }

  /**
   * Return an estimate of the number of instances the fragment with this sink will run
   * on. This is based on the number of instances or nodes of the join node, since they
   * are co-located, but the build may be shared.
   */
  public int getNumInstances() {
    return joinNode_.canShareBuild() ?
        joinNode_.getFragment().getNumNodes() * numBuildWorkers_ :
        joinNode_.getFragment().getNumInstances();
  }

  public int getNumBuildWorkers() { return numBuildWorkers_; }

  public boolean isShared() { return joinNode_.canShareBuild(); }

  @Override
  protected String getLabel() {
    return "JOIN BUILD";
  }

  @Override
  public void computeProcessingCost(TQueryOptions queryOptions) {
    // The processing cost to export rows.
    processingCost_ = joinNode_.computeJoinProcessingCost().second;
  }

  @Override
  public void computeResourceProfile(TQueryOptions queryOptions) {
    resourceProfile_ = joinNode_.computeJoinResourceProfile(queryOptions).second;
  }

  @Override
  public void computeResourceProfileIfSpill(
      TQueryOptions queryOptions, long maxMemoryEstimatePerInstance) {
    if (joinNode_ instanceof HashJoinNode) {
      // Only HashJoinNode implements SpillableOperator.
      resourceProfile_ =
          ((HashJoinNode) joinNode_)
              .computeJoinResourceProfile(queryOptions, maxMemoryEstimatePerInstance)
              .second;
    } else {
      computeResourceProfile(queryOptions);
    }
  }

  @Override
  public void collectExprsForLineage(List<Expr> exprs) {
    exprs.addAll(buildExprs_);
  }

  @Override
  public void computeRowConsumptionAndProductionToCost() {
    super.computeRowConsumptionAndProductionToCost();
    if (isShared()) {
      fragment_.setFixedInstanceCount(getNumInstances());
    }
  }
}
