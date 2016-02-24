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

import edu.snu.mist.common.DAG;
import edu.snu.mist.task.OperatorChain;
import edu.snu.mist.task.operators.StatefulOperator;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.SourceGenerator;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

public interface QueryContent {

  public enum QueryStatus {
    INACTIVE,
    UNLOADING,
    PARTIALLY_ACTIVE,
    LOADING,
    ACTIVE,
  }

  void setLatestActiveTime(long latestActiveTime);

  long getLatestActiveTime();

  void setQueryInfo(Map<SourceGenerator, Set<OperatorChain>> sourceMap,
                           DAG<OperatorChain> operatorChains,
                           Map<OperatorChain, Set<Sink>> sinkMap);

  QueryStatus getQueryStatus();

  void setQueryStatus(QueryStatus queryStatus);

  Queue getQueue();

  Set<StatefulOperator> getStatefulOperators();

  String getQueryId();

  Map<SourceGenerator, Set<OperatorChain>> getSourceMap();

  void clearQueryInfo();
}
