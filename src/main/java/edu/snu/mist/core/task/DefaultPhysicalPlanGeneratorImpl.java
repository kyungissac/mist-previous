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
package edu.snu.mist.core.task;

import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.PhysicalVertex;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.EventGenerator;
import edu.snu.mist.common.sources.Source;
import edu.snu.mist.common.sources.SourceImpl;
import edu.snu.mist.core.parameters.TempFolderPath;
import edu.snu.mist.formats.avro.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * A default implementation of PhysicalPlanGenerator.
 */
final class DefaultPhysicalPlanGeneratorImpl implements PhysicalPlanGenerator {

  private static final Logger LOG = Logger.getLogger(DefaultPhysicalPlanGeneratorImpl.class.getName());

  private final OperatorIdGenerator operatorIdGenerator;
  private final String tmpFolderPath;
  private final ClassLoaderProvider classLoaderProvider;
  private final PhysicalObjectGenerator physicalObjectGenerator;
  private final StringIdentifierFactory identifierFactory;

  @Inject
  private DefaultPhysicalPlanGeneratorImpl(final OperatorIdGenerator operatorIdGenerator,
                                           @Parameter(TempFolderPath.class) final String tmpFolderPath,
                                           final StringIdentifierFactory identifierFactory,
                                           final ClassLoaderProvider classLoaderProvider,
                                           final PhysicalObjectGenerator physicalObjectGenerator) {
    this.operatorIdGenerator = operatorIdGenerator;
    this.tmpFolderPath = tmpFolderPath;
    this.classLoaderProvider = classLoaderProvider;
    this.identifierFactory = identifierFactory;
    this.physicalObjectGenerator = physicalObjectGenerator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public DAG<PhysicalVertex, Direction> generate(
      final Tuple<String, LogicalPlan> queryIdAndLogicalPlan)
      throws IllegalArgumentException, IOException, ClassNotFoundException, InjectionException {
    final LogicalPlan logicalPlan = queryIdAndLogicalPlan.getValue();
    final List<PhysicalVertex> deserializedVertices = new ArrayList<>();
    final DAG<PhysicalVertex, Direction> dag = new AdjacentListDAG<>();

    // Get a class loader
    final URL[] urls = SerializeUtils.getURLs(logicalPlan.getJarFilePaths());
    final ClassLoader classLoader = classLoaderProvider.newInstance(urls);

    // Deserialize vertices
    for (final AvroVertexChain avroVertexChain : logicalPlan.getAvroVertices()) {
      switch (avroVertexChain.getAvroVertexChainType()) {
        case SOURCE: {
          final Vertex vertex = avroVertexChain.getVertexChain().get(0);
          // Create an event generator
          final EventGenerator eventGenerator = physicalObjectGenerator.newEventGenerator(
              vertex.getConfiguration(), classLoader, urls);
          // Create a data generator
          final DataGenerator dataGenerator = physicalObjectGenerator.newDataGenerator(
              vertex.getConfiguration(), classLoader, urls);
          // Create a source
          final Source source = new SourceImpl<>(
              identifierFactory.getNewInstance(operatorIdGenerator.generate()),
              dataGenerator, eventGenerator);
          deserializedVertices.add(source);
          dag.addVertex(source);
          break;
        }
        case OPERATOR_CHAIN: {
          final PartitionedQuery partitionedQuery = new DefaultPartitionedQuery();
          deserializedVertices.add(partitionedQuery);
          for (final Vertex vertex : avroVertexChain.getVertexChain()) {
            final Operator operator = physicalObjectGenerator.newOperator(
                operatorIdGenerator.generate(), vertex.getConfiguration(), classLoader, urls);
            partitionedQuery.insertToTail(operator);
          }
          dag.addVertex(partitionedQuery);
          break;
        }
        case SINK: {
          final Vertex vertex = avroVertexChain.getVertexChain().get(0);
          final Sink sink = physicalObjectGenerator.newSink(
              operatorIdGenerator.generate(), vertex.getConfiguration(), classLoader, urls);
          deserializedVertices.add(sink);
          dag.addVertex(sink);
          break;
        }
        default: {
          throw new IllegalArgumentException("MISTTask: Invalid vertex detected in LogicalPlan!");
        }
      }
    }

    // Add edge info to physical plan
    for (final Edge edge : logicalPlan.getEdges()) {
      final int srcIndex = edge.getFrom();
      final PhysicalVertex deserializedSrcVertex = deserializedVertices.get(srcIndex);
      final int dstIndex = edge.getTo();
      final PhysicalVertex deserializedDstVertex = deserializedVertices.get(dstIndex);
      dag.addEdge(deserializedSrcVertex, deserializedDstVertex, edge.getDirection());
    }
    return dag;
  }
}