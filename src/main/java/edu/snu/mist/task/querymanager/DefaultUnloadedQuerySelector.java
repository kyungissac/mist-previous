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

import edu.snu.mist.task.parameters.MaxWaitTimeThreshold;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

public final class DefaultUnloadedQuerySelector implements UnloadedQuerySelector {

  private final QueryContentComparator comparator;

  private final int maxWaitTimeThreshold;

  @Inject
  private DefaultUnloadedQuerySelector(
      final QueryContentComparator comparator,
      @Parameter(MaxWaitTimeThreshold.class) final int maxWaitTimeThreshold) {
    this.comparator = comparator;
    this.maxWaitTimeThreshold = maxWaitTimeThreshold;
  }

  public List<QueryContent> selectUnloadedQueries(final ConcurrentMap<String, QueryContent> queryMap) {
    // sort by latest active time
    final Queue<QueryContent> queue = new PriorityQueue<>(comparator);
    queue.addAll(queryMap.values());

    // select unloading queries
    final List<QueryContent> unloadingQueries = new LinkedList<>();
    for (final QueryContent queryContent : queue) {
      if (System.currentTimeMillis() - queryContent.getLatestActiveTime() > maxWaitTimeThreshold) {
        final AtomicReference<QueryContent.QueryStatus> queryStatus = queryContent.getQueryStatus();
        if (queryStatus.get() == QueryContent.QueryStatus.ACTIVE) {
          if (queryStatus.compareAndSet(QueryContent.QueryStatus.ACTIVE, QueryContent.QueryStatus.UNLOADING)) {
            unloadingQueries.add(queryContent);
          }
        }
      }
    }
    return unloadingQueries;
  }
}
