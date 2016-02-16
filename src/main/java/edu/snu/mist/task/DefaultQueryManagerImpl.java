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

import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.parameters.GracePeriod;
import edu.snu.mist.task.querystore.QueryStore;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

final class DefaultQueryManagerImpl implements QueryManager {

  private final ConcurrentMap<String, QueryContent> queryInfoMap;

  private final QueryStore queryStore;

  private final EventHandler<String> deleteCallback;

  private final AtomicLong lastCheckpoint;

  private final long gracePeriod;

  private final ScheduledExecutorService scheduledExecutorService;
  @Inject
  private DefaultQueryManagerImpl(final QueryStore queryStore,
                                  @Parameter(GracePeriod.class) final long gracePeriod) {
    this.queryInfoMap = new ConcurrentHashMap<>();
    this.queryStore = queryStore;
    this.deleteCallback = deletedId -> {};
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        // checkpoint and unload states/info from memory.

      }
    }, gracePeriod, gracePeriod, TimeUnit.MILLISECONDS);
    this.lastCheckpoint = new AtomicLong(System.currentTimeMillis());
    this.gracePeriod = gracePeriod;
  }

  @Override
  public QueryContent createQueryContent(final String queryId,
                                         final PhysicalPlan<OperatorChain> physicalPlan) {
    final QueryContent queryContent = new DefaultQueryContentImpl(queryId,
        physicalPlan.getSourceMap(), physicalPlan.getOperators(), physicalPlan.getSinkMap());
    queryInfoMap.put(queryId, queryContent);
    // TODO[MIST-#]: Deserialize physical plan and store query info and state to QueryStore.
    return queryContent;
  }

  @Override
  public void deleteQueryInfo(final String queryId) {
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

  @Override
  public void emit(final SourceInput sourceInput) {
    final QueryContent queryContent = queryInfoMap.get(sourceInput.getQueryId());
    final Queue queue = queryContent.getQueue();
    final Object input = sourceInput.getInput();
    final Set<OperatorChain> nextOps = queryContent.getSourceMap().get(sourceInput.getSrc());

    switch (queryContent.getQueryStatus()) {
      case ACTIVE:
        while (queue.isEmpty()) {
          // if query is active but queue is not empty, wait until the queue becomes empty.
          synchronized (queue) {
            try {
              queue.wait();
            } catch (final InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
        forwardInput(nextOps, input);
        break;
      case PARTIALLY_ACTIVE:
        // Load states of StatefulOperators.
        // After loading the states, it should set the query status to ACTIVE
        // and forwards the inputs in the queue to next operators.
        queryContent.setQueryStatus(QueryContent.QueryStatus.ACTIVE);
        // TODO[MIST-#]: Load states of a query.
        queue.add(input);
        break;
      case INACTIVE:
        // Load states and info of the query.
        // After loading the states and info, it should set the query status to ACTIVE
        // and forwards the inputs in the queue to next operators.
        queryContent.setQueryStatus(QueryContent.QueryStatus.ACTIVE);
        // TODO[MIST-#]: Load info of a query.
        queue.add(input);
        break;
      default:
        throw new RuntimeException("Invalid query status");
    }
  }
}
