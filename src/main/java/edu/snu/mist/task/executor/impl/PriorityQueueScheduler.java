/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.mist.task.executor.impl;

import edu.snu.mist.task.executor.ExecutorTask;

import javax.inject.Inject;

/**
 * PriorityQueueScheduler schedules executor tasks in executor.
 */
public final class PriorityQueueScheduler implements SimpleMistExecutor.SimpleExecutorScheduler {

  @Inject
  private PriorityQueueScheduler() {

  }

  @Override
  public <I> void setPriorityOfExecutorTask(final ExecutorTask<I> executorTask) {

  }

  @Override
  public int compare(final Runnable t1, final Runnable t2) {
    final ExecutorTask et1 = (ExecutorTask)t1;
    final ExecutorTask et2 = (ExecutorTask)t2;

    if (et1.getPriority() < et2.getPriority()) {
      return 1;
    } else if (et1.getPriority() > et2.getPriority()) {
      return -1;
    } else {
      return 0;
    }
  }
}
