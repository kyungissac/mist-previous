/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.core.task.queryRemovers;

import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.*;

import javax.inject.Inject;
import java.util.Collection;

/**
 * This removes the query from MIST.
 * It considers the queries are merged and vertices have their reference count.
 * So, this remover will decrease the reference count of the physical vertices
 * and delete them when it becomes zero.
 */
final class MergeAwareQueryRemover implements QueryRemover {

  /**
   * Execution dags that are currently running.
   */
  private final ExecutionDags<String> executionDags;

  /**
   * The map that has the query id as a key and its execution dag as a value.
   */
  private final ExecutionPlanDagMap executionPlanDagMap;

  /**
   * Vertex info map that has the execution vertex as a key and the vertex info as a value.
   */
  private final VertexInfoMap vertexInfoMap;

  @Inject
  private MergeAwareQueryRemover(final ExecutionPlanDagMap executionPlanDagMap,
                                 final ExecutionDags<String> executionDags,
                                 final VertexInfoMap vertexInfoMap) {
    this.executionDags = executionDags;
    this.executionPlanDagMap = executionPlanDagMap;
    this.vertexInfoMap = vertexInfoMap;
  }

  /**
   * Delete the query from the group.
   * @param queryId query id
   */
  @Override
  public synchronized void deleteQuery(final String queryId) {
    // Synchronize the execution dags to evade concurrent modifications
    // TODO:[MIST-590] We need to improve this code for concurrent modification
    synchronized (executionDags) {
      // Delete the query plan from ExecutionPlanDagMap
      final DAG<ExecutionVertex, MISTEdge> executionPlan = executionPlanDagMap.remove(queryId);
      // Delete vertices from vertex info map
      final Collection<ExecutionVertex> vertices = executionPlan.getVertices();
      for (final ExecutionVertex vertex : vertices) {
        final VertexInfo vertexInfo = vertexInfoMap.remove(vertex);
        vertexInfo.setRefCount(vertexInfo.getRefCount() - 1);
        if (vertexInfo.getRefCount() == 0) {
          // Delete it from the physical dag
          final DAG<ExecutionVertex, MISTEdge> targetDag = vertexInfo.getPhysicalExecutionDag();
          final ExecutionVertex deleteVertex = vertexInfo.getPhysicalExecutionVertex();
          targetDag.removeVertex(deleteVertex);
          // Stop if it is source
          if (deleteVertex.getType() == ExecutionVertex.Type.SOURCE) {
            final PhysicalSource src = (PhysicalSource)deleteVertex;
            executionDags.remove(src.getConfiguration());
            try {
              src.close();
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }
      }
    }
  }
}