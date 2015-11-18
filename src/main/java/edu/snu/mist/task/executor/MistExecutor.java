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
package edu.snu.mist.task.executor;

import org.apache.reef.wake.EStage;

/**
 * Mist Executor runs multiple queries on single thread.
 */
public interface MistExecutor extends EStage<ExecutorTask> {

  /**
   * Receives an ExecutorTask which computes inputs according to the assigned operators.
   * @param executorTask an executor task
   */
  @Override
  void onNext(ExecutorTask executorTask);

  /**
   * Gets current load of the executor.
   * This can be used for load balance.
   * @return current load
   */
  int getCurrentLoad();
}
