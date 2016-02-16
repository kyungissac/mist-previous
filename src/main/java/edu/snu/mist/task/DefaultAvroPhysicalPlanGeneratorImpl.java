/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.task;

import edu.snu.mist.formats.avro.*;
import edu.snu.mist.task.operators.FilterOperator;
import edu.snu.mist.task.operators.FlatMapOperator;
import edu.snu.mist.task.operators.MapOperator;
import edu.snu.mist.task.operators.ReduceByKeyOperator;
import edu.snu.mist.task.sinks.TextSocketSink;
import edu.snu.mist.task.sources.TextSocketStreamGenerator;
import org.apache.reef.io.Tuple;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * A default implementation of AvroPhysicalPlanGenerator.
 */
final class DefaultAvroPhysicalPlanGeneratorImpl implements AvroPhysicalPlanGenerator {

  private static final Logger LOG = Logger.getLogger(DefaultAvroPhysicalPlanGeneratorImpl.class.getName());

  private final OperatorIdGenerator operatorIdGenerator;

  @Inject
  private DefaultAvroPhysicalPlanGeneratorImpl(final OperatorIdGenerator operatorIdGenerator) {
    this.operatorIdGenerator = operatorIdGenerator;
  }

  private List<String> findParallelismAndImplementation(final Vertex vertex) {
    switch (vertex.getVertexType()) {
      case SOURCE:
        final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
        return findSourceParallelismAndImplementation(sourceInfo);
      case INSTANT_OPERATOR:
        final InstantOperatorInfo iOpInfo = (InstantOperatorInfo) vertex.getAttributes();
        return findInstantOperatorParallelismAndImplementation(iOpInfo);
      case WINDOW_OPERATOR:
        throw new IllegalArgumentException("MISTTask: WindowOperator is currently not supported!");
      case SINK:
        final SinkInfo sinkInfo = (SinkInfo) vertex.getAttributes();
        return findSinkParallelismAndImplementation(sinkInfo);
      default:
        throw new IllegalArgumentException("MISTTask: Invalid vertex detected in LogicalPlan!");
    }
  }

  private List<String> findSourceParallelismAndImplementation(final SourceInfo sourceInfo) {
    final List<String> list = new LinkedList<>();
    switch (sourceInfo.getSourceType()) {
      case TEXT_SOCKET_SOURCE:
        list.add(TextSocketStreamGenerator.class.getName());
        return list;
      case REEF_NETWORK_SOURCE:
        throw new IllegalArgumentException("MISTTask: REEF_NETWORK_SOURCE is currently not supported!");
      default:
        throw new IllegalArgumentException("MISTTask: Invalid source generator detected in LogicalPlan!");
    }
  }

  private List<String> findInstantOperatorParallelismAndImplementation(final InstantOperatorInfo iOpInfo) {
    final List<String> list = new LinkedList<>();
    switch (iOpInfo.getInstantOperatorType()) {
      case APPLY_STATEFUL:
        throw new IllegalArgumentException("MISTTask: ApplyStatefulOperator is currently not supported!");
      case FILTER:
        list.add(FilterOperator.class.getName());
        return list;
      case FLAT_MAP:
        list.add(FlatMapOperator.class.getName());
        return list;
      case MAP:
        list.add(MapOperator.class.getName());
        return list;
      case REDUCE_BY_KEY:
        list.add(ReduceByKeyOperator.class.getName());
        return list;
      case REDUCE_BY_KEY_WINDOW:
        throw new IllegalArgumentException("MISTTask: ReduceByKeyWindowOperator is currently not supported!");
      default:
        throw new IllegalArgumentException("MISTTask: Invalid InstantOperatorType detected!");
    }
  }

  private List<String> findSinkParallelismAndImplementation(final SinkInfo sinkInfo) {
    final List<String> list = new LinkedList<>();
    switch (sinkInfo.getSinkType()) {
      case TEXT_SOCKET_SINK:
        list.add(TextSocketSink.class.getName());
        return list;
      case REEF_NETWORK_SINK:
        throw new IllegalArgumentException("MISTTask: REEF_NETWORK_SINK is currently not supported!");
      default:
        throw new IllegalArgumentException("MISTTask: Invalid sink detected in LogicalPlan!");
    }
  }

  @Override
  public AvroPhysicalPlan generate(final Tuple<String, LogicalPlan> queryIdAndLogicalPlan)
      throws IllegalArgumentException {
    final String queryId = queryIdAndLogicalPlan.getKey();
    final LogicalPlan logicalPlan = queryIdAndLogicalPlan.getValue();
    final Map<Integer, Tuple<String, PhysicalVertex.Builder>> builderMap = new HashMap<>();
    final List<Vertex> logicalVertices = logicalPlan.getVertices();
    final List<List<Tuple<String, PhysicalVertex.Builder>>> parallelizedVertices = new LinkedList<>();

    // Convert logical plan into physical plan
    for (final Vertex vertex : logicalVertices) {
      final List<String> implementations = findParallelismAndImplementation(vertex);
      final List<Tuple<String, PhysicalVertex.Builder>> parallelizedVertex = new LinkedList<>();
      parallelizedVertices.add(parallelizedVertex);
      for (final String className : implementations) {
        final String physicalVertexId = operatorIdGenerator.generate();
        final PhysicalVertex.Builder physicalVertexBuilder = PhysicalVertex.newBuilder();
        physicalVertexBuilder.setPhysicalVertexClass(className);
        physicalVertexBuilder.setLogicalInfo(vertex);
        builderMap.put(logicalVertices.indexOf(vertex),
            new Tuple<>(physicalVertexId, physicalVertexBuilder));
        parallelizedVertex.add(new Tuple<>(physicalVertexId, physicalVertexBuilder));
      }
    }

    // Connect physical vertices
    for (final Edge edge : logicalPlan.getEdges()) {
      final int srcIndex = edge.getFrom();
      final int dstIndex = edge.getTo();
      final List<Tuple<String, PhysicalVertex.Builder>>
          srcParallelizedVertex = parallelizedVertices.get(srcIndex);
      final List<Tuple<String, PhysicalVertex.Builder>>
          dstParallelizedVertex = parallelizedVertices.get(dstIndex);
      // TODO[MIST-#]: Need partitioning policy.
      // Currently, this source vertex broadcasts data to dest vertices.
      // We should handle this issue when we consider parallelism.
      for (final Tuple<String, PhysicalVertex.Builder> srcTuple : srcParallelizedVertex) {
        final List<CharSequence> dstVerticesIds = new LinkedList<>();
        for (final Tuple<String, PhysicalVertex.Builder> dstTuple : dstParallelizedVertex) {
          dstVerticesIds.add(dstTuple.getKey());
        }
        srcTuple.getValue().setEdges(dstVerticesIds);
      }
    }

    // Create AvroPhysicalBuilder
    AvroPhysicalPlan.Builder avroPhysicalPlanBuilder = AvroPhysicalPlan.newBuilder();
    final Map<CharSequence, PhysicalVertex> result = new HashMap<>();
    for (final List<Tuple<String, PhysicalVertex.Builder>> list : parallelizedVertices) {
      for (final Tuple<String, PhysicalVertex.Builder> tuple : list) {
        if (!tuple.getValue().hasEdges()) {
          tuple.getValue().setEdges(new LinkedList<>());
        }
        result.put(tuple.getKey(), tuple.getValue().build());
      }
    }
    avroPhysicalPlanBuilder.setQueryId(queryId);
    avroPhysicalPlanBuilder.setPhysicalVertices(result).build();
    return avroPhysicalPlanBuilder.build();
  }
}