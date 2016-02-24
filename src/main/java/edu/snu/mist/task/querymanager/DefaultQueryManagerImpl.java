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

package edu.snu.mist.task.querymanager;

import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.formats.avro.QueryState;
import edu.snu.mist.task.*;
import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.operators.StatefulOperator;
import edu.snu.mist.task.querymanager.querystores.QueryStore;
import edu.snu.mist.utils.AvroSerializer;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import javax.management.Notification;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class DefaultQueryManagerImpl implements QueryManager {

  private final ConcurrentMap<String, QueryContent> queryInfoMap;

  private final QueryStore queryStore;

  private final EventHandler<String> deleteCallback;

  private final UnloadedQuerySelector unloadedQuerySelector;

  @Inject
  private DefaultQueryManagerImpl(final QueryStore queryStore,
                                  final UnloadedQuerySelector unloadedQuerySelector) {
    this.queryInfoMap = new ConcurrentHashMap<>();
    this.queryStore = queryStore;
    this.deleteCallback = deletedId -> {};
    this.unloadedQuerySelector = unloadedQuerySelector;
  }

  @Override
  public void registerQuery(final String queryId,
                            final PhysicalPlan<OperatorChain> physicalPlan,
                            final LogicalPlan logicalPlan) {
    final QueryContent queryContent = new DefaultQueryContentImpl(queryId,
        physicalPlan.getSourceMap(), physicalPlan.getOperators(), physicalPlan.getSinkMap());
    queryInfoMap.put(queryId, queryContent);
    // Store logical plan
    //queryStore.storeLogicalPlan(queryId, AvroSerializer.avroToString(logicalPlan, LogicalPlan.class), null);
  }

  @Override
  public void unregisterQuery(final String queryId) {
    queryInfoMap.remove(queryId);
    queryStore.deleteQuery(queryId, deleteCallback);
  }

  @Override
  public void close() throws Exception {
  }

  private void forwardInput(final Set<OperatorChain> nextOperators, final Object input) {
    for (final OperatorChain nextOp : nextOperators) {
      final MistExecutor executor = nextOp.getExecutor();
      final OperatorChainJob operatorChainJob = new DefaultOperatorChainJob(nextOp, input);
      // Always submits a job to the MistExecutor when inputs are received.
      executor.submit(operatorChainJob);
    }
  }

  /**
   * Receive source input from source generator.
   * @param sourceInput source input
   */
  @Override
  public void emit(final SourceInput sourceInput) {
    final QueryContent queryContent = queryInfoMap.get(sourceInput.getQueryId());
    final Queue queue = queryContent.getQueue();
    final Object input = sourceInput.getInput();
    final Set<OperatorChain> nextOps = queryContent.getSourceMap().get(sourceInput.getSrc());
    final long currTime = System.currentTimeMillis();

    synchronized (queryContent) {
      queryContent.setLatestActiveTime(currTime);
      switch (queryContent.getQueryStatus()) {
        case ACTIVE:
          if (!queue.isEmpty()) {
            // if query is active but queue is not empty, add the input to the queue.
            queue.add(input);
          } else {
            forwardInput(nextOps, input);
          }
          break;
        case PARTIALLY_ACTIVE:
          // Load states of StatefulOperators.
          // After loading the states, it should set the query status to ACTIVE
          // and forwards the inputs in the queue to next operators.
          queue.add(input);
          //queryContent.setQueryStatus(QueryContent.QueryStatus.ACTIVE);
          // TODO[MIST-#]: Load states of a query.
          break;
        case INACTIVE:
          // Load states and info of the query.
          // After loading the states and info, it should set the query status to ACTIVE
          // and forwards the inputs in the queue to next operators.
          queue.add(input);
          //queryContent.setQueryStatus(QueryContent.QueryStatus.ACTIVE);
          // TODO[MIST-#]: Load info of a query.
          break;
        default:
          throw new RuntimeException("Invalid query status");
      }
    }
  }

  @Override
  public void handleNotification(final Notification notification, final Object handback) {
    final List<QueryContent> queriesToBeUnloaded =
        unloadedQuerySelector.selectUnloadedQueries(queryInfoMap);
    // Serialize query states
    for (final QueryContent queryContent : queriesToBeUnloaded) {
      synchronized (queryContent) {
        final Set<StatefulOperator> statefulOperators = queryContent.getStatefulOperators();
        final QueryState.Builder stateBuilder = QueryState.newBuilder();
        stateBuilder.setQueryId(queryContent.getQueryId());
        final Map<CharSequence, ByteBuffer> stateMap = new HashMap<>();
        for (final StatefulOperator statefulOperator : statefulOperators) {
          stateMap.put(statefulOperator.getOperatorIdentifier().toString(),
              ByteBuffer.wrap(SerializationUtils.serialize(statefulOperator.getState())));
        }
        stateBuilder.setOperatorStateMap(stateMap);
        // store query state
        queryStore.storeState(queryContent.getQueryId(),
            AvroSerializer.avroToString(stateBuilder.build(), QueryState.class), null);
        // unload query
        queryContent.clearQueryInfo();
        queryContent.setQueryStatus(QueryContent.QueryStatus.PARTIALLY_ACTIVE);
      }
    }
  }
}
