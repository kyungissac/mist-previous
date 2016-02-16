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

import edu.snu.mist.api.StreamType;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.GraphUtils;
import edu.snu.mist.task.operators.Operator;
import edu.snu.mist.task.operators.StatefulOperator;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.SourceGenerator;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

final class DefaultQueryContentImpl implements QueryContent {

  private final AtomicReference<QueryStatus> status;

  private final String queryId;

  private Map<SourceGenerator, Set<OperatorChain>> sources;

  private DAG<OperatorChain> operators;

  private Map<OperatorChain, Set<Sink>> sinks;

  private Set<StatefulOperator> statefulOperators;

  private final Queue<Object> queue;

  DefaultQueryContentImpl(final String queryId,
                          final Map<SourceGenerator, Set<OperatorChain>> sourceMap,
                          final DAG<OperatorChain> operatorChains,
                          final Map<OperatorChain, Set<Sink>> sinkMap) {
    this.status = new AtomicReference<>(QueryStatus.ACTIVE);
    this.queryId = queryId;
    this.sources = sourceMap;
    this.operators = operatorChains;
    this.sinks = sinkMap;
    this.queue = new LinkedBlockingQueue<>();
  }

  @Override
  public void setQueryInfo(final Map<SourceGenerator, Set<OperatorChain>> sourceMap,
                           final DAG<OperatorChain> operatorChains,
                           final Map<OperatorChain, Set<Sink>> sinkMap) {
    this.sources = sourceMap;
    this.operators = operatorChains;
    this.sinks = sinkMap;
    statefulOperators = new HashSet<>();
    final Iterator<OperatorChain> iterator = GraphUtils.topologicalSort(operatorChains);
    while (iterator.hasNext()) {
      final OperatorChain operatorChain = iterator.next();
      for (final Operator operator : operatorChain.getOperators()) {
        final StreamType.OperatorType operatorType = operator.getOperatorType();
        // check whether the operator is stateful or not.
        if (operatorType == StreamType.OperatorType.REDUCE_BY_KEY ||
            operatorType == StreamType.OperatorType.APPLY_STATEFUL ||
            operatorType == StreamType.OperatorType.REDUCE_BY_KEY_WINDOW) {
          // this operator is stateful
          final StatefulOperator statefulOperator = (StatefulOperator)operator;
          statefulOperators.add(statefulOperator);
        }
      }
    }
  }

  @Override
  public QueryStatus getQueryStatus() {
    return status.get();
  }

  @Override
  public void setQueryStatus(QueryStatus queryStatus) {
    status.compareAndSet(status.get(), queryStatus);
  }

  @Override
  public Queue getQueue() {
    return queue;
  }

  @Override
  public Map<SourceGenerator, Set<OperatorChain>> getSourceMap() {
    return sources;
  }

  @Override
  public Set<StatefulOperator> getStatefulOperators() {
    return statefulOperators;
  }

  @Override
  public String getQueryId() {
    return queryId;
  }

  @Override
  public void clearQueryInfo() {
    if (status.get() == QueryStatus.INACTIVE) {
      sources = null;
      operators = null;
      statefulOperators = null;
      sinks = null;
    } else {
      // warning
    }
  }
}
