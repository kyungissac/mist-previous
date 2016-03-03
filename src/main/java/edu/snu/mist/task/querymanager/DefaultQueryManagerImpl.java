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

import edu.snu.mist.formats.avro.AvroPhysicalPlan;
import edu.snu.mist.formats.avro.QueryState;
import edu.snu.mist.task.*;
import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.operators.Operator;
import edu.snu.mist.task.operators.StatefulOperator;
import edu.snu.mist.task.sources.SourceInput;
import edu.snu.mist.utils.AvroSerializer;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import javax.management.Notification;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

final class DefaultQueryManagerImpl implements QueryManager {

  private final ConcurrentMap<String, QueryContent> queryInfoMap;

  private final QueryStore queryStore;

  private final EventHandler<String> deleteCallback;

  private final UnloadedQuerySelector unloadedQuerySelector;

  private final PhysicalPlanGenerator physicalPlanGenerator;

  private final OperatorChainer operatorChainer;

  private final OperatorChainAllocator operatorChainAllocator;

  @Inject
  private DefaultQueryManagerImpl(final QueryStore queryStore,
                                  final UnloadedQuerySelector unloadedQuerySelector,
                                  final PhysicalPlanGenerator physicalPlanGenerator,
                                  final OperatorChainer operatorChainer,
                                  final OperatorChainAllocator operatorChainAllocator) {
    this.queryInfoMap = new ConcurrentHashMap<>();
    this.queryStore = queryStore;
    this.deleteCallback = deletedId -> {};
    this.unloadedQuerySelector = unloadedQuerySelector;
    this.physicalPlanGenerator = physicalPlanGenerator;
    this.operatorChainer = operatorChainer;
    this.operatorChainAllocator = operatorChainAllocator;
  }

  @Override
  public void registerQuery(final String queryId,
                            final PhysicalPlan<OperatorChain> physicalPlan,
                            final AvroPhysicalPlan avroPhysicalPlan) {
    final QueryContent queryContent = new DefaultQueryContentImpl(queryId,
        physicalPlan.getSourceMap(), physicalPlan.getOperators(), physicalPlan.getSinkMap());
    queryInfoMap.put(queryId, queryContent);
    // Store serialized plan
    queryStore.storeLogicalPlan(queryId,
        AvroSerializer.avroToString(avroPhysicalPlan, AvroPhysicalPlan.class), null);
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

  private void getQueryStateAndForwardInputs(final QueryContent queryContent, final Set<OperatorChain> nextOps) {
    final String queryId = queryContent.getQueryId();
    final Queue queue = queryContent.getQueue();
    queryStore.getState(queryId, (tuple) -> {
      // deserialize
      final QueryState queryState = AvroSerializer.avroFromString(
          tuple.getValue(), QueryState.getClassSchema(), QueryState.class);
      final Set<StatefulOperator> statefulOperators = queryContent.getStatefulOperators();
      for (final StatefulOperator statefulOperator : statefulOperators) {
        final String operatorId = statefulOperator.getOperatorIdentifier().toString();
        final ByteBuffer serializedState = queryState.getOperatorStateMap().get(operatorId);
        statefulOperator.setState((Serializable)SerializationUtils.deserialize(serializedState.array()));
      }
      synchronized (queryContent) {
        queryContent.setQueryStatus(QueryContent.QueryStatus.ACTIVE);
        while (!queue.isEmpty()) {
          final Object i = queue.poll();
          forwardInput(nextOps, i);
        }
      }
    });
  }

  /**
   * Receive source input from source generator.
   * @param sourceInput source input
   */
  @Override
  public void emit(final SourceInput sourceInput) {
    final String queryId = sourceInput.getQueryIdentifier().toString();
    final QueryContent queryContent = queryInfoMap.get(queryId);
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
          queryContent.setQueryStatus(QueryContent.QueryStatus.LOADING);
          queue.add(input);
          getQueryStateAndForwardInputs(queryContent, nextOps);
          break;
        case INACTIVE:
          // Load states and info of the query.
          // After loading the states and info, it should set the query status to ACTIVE
          // and forwards the inputs in the queue to next operators.
          queryContent.setQueryStatus(QueryContent.QueryStatus.LOADING);
          queue.add(input);
          queryStore.getInfo(queryId, (infoTuple) -> {
            final AvroPhysicalPlan avroPhysicalPlan = AvroSerializer.avroFromString(infoTuple.getValue(),
                AvroPhysicalPlan.getClassSchema(), AvroPhysicalPlan.class);
            try {
              final PhysicalPlan<Operator> physicalPlan = physicalPlanGenerator.generate(avroPhysicalPlan);
              final PhysicalPlan<OperatorChain> chainPhysicalPlan = operatorChainer.chainOperators(physicalPlan);
              operatorChainAllocator.allocate(chainPhysicalPlan.getOperators());
              registerQuery(queryId, chainPhysicalPlan, avroPhysicalPlan);
              getQueryStateAndForwardInputs(queryContent, nextOps);
            } catch (final InjectionException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          });
          break;
        case LOADING:
          queue.add(input);
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
        switch (queryContent.getQueryStatus()) {
          case ACTIVE:
            // Serialize and unload states
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
            queryContent.setQueryStatus(QueryContent.QueryStatus.PARTIALLY_ACTIVE);
            break;
          case PARTIALLY_ACTIVE:
            // unload query
            // Currently we suppose that query info is not changed.
            queryContent.clearQueryInfo();
            queryContent.setQueryStatus(QueryContent.QueryStatus.INACTIVE);
            break;
          default:
            throw new RuntimeException("Invalid Query Status for query unload: " + queryContent.getQueryStatus());
        }
      }
    }
  }
}