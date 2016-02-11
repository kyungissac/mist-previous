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

import javax.inject.Inject;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class DefaultQueryManagerImpl implements QueryManager {

  private final ConcurrentMap<String, QueryInfo> queryInfoMap;

  @Inject
  private DefaultQueryManagerImpl() {
    this.queryInfoMap = new ConcurrentHashMap<>();
  }

  @Override
  public QueryInfo createQueryInfo(final String queryId,
                                   final PhysicalPlan<OperatorChain> physicalPlan) {
    final QueryInfo queryInfo = new DefaultQueryInfoImpl(queryId,
        physicalPlan.getSourceMap(), physicalPlan.getOperators(), physicalPlan.getSinkMap());
    queryInfoMap.put(queryId, queryInfo);
    return queryInfo;
  }

  @Override
  public void deleteQueryInfo(final String queryId) {
    queryInfoMap.remove(queryId);
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public void emit(final SourceInput sourceInput) {
    final QueryInfo queryInfo = queryInfoMap.get(sourceInput.getQueryId());
    final Queue queue = queryInfo.getQueue();
    final Object input = sourceInput.getInput();
    if (queryInfo.getQueryStatus() == QueryInfo.QueryStatus.ACTIVE) {
      final Set<OperatorChain> nextOps = queryInfo.getSourceMap().get(sourceInput.getSrc());
      while (!queue.isEmpty()) {
        final Object prevInput = queue.poll();
        for (final OperatorChain nextOp : nextOps) {
          final MistExecutor executor = nextOp.getExecutor();
          final OperatorChainJob operatorChainJob = new DefaultOperatorChainJob(nextOp, prevInput);
          // Always submits a job to the MistExecutor when inputs are received.
          executor.submit(operatorChainJob);
        }
      }

      for (final OperatorChain nextOp : nextOps) {
        final MistExecutor executor = nextOp.getExecutor();
        final OperatorChainJob operatorChainJob = new DefaultOperatorChainJob(nextOp, input);
        // Always submits a job to the MistExecutor when inputs are received.
        executor.submit(operatorChainJob);
      }
    } else {
      queue.add(input);
    }
  }
}
