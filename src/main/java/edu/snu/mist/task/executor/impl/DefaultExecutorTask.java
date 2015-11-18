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

import com.google.common.collect.ImmutableList;
import edu.snu.mist.task.executor.ExecutorTask;
import edu.snu.mist.task.operator.Operator;

/**
 * Default implementation of executor task.
 * @param <I> input type
 */
public final class DefaultExecutorTask<I> implements ExecutorTask<I> {

  /**
   * An operator for the task.
   */
  private final Operator<I, ?> operator;

  /**
   * A list of inputs of the operator.
   */
  private final ImmutableList<I> inputs;

  /**
   * Priority of the task.
   */
  private int priority;

  public DefaultExecutorTask(final Operator<I, ?> operator,
                             final ImmutableList<I> inputs) {
    this(operator, inputs, 0);
  }

  public DefaultExecutorTask(final Operator<I, ?> operator,
                             final ImmutableList<I> inputs,
                             final int priority) {
    this.operator = operator;
    this.inputs = inputs;
    this.priority = priority;
  }

  /**
   * Gets the priority of the task.
   * @return
   */
  @Override
  public int getPriority() {
    return priority;
  }

  /**
   * Sets the priority of the task.
   * @param prior priority
   */
  @Override
  public void setPriority(final int prior) {
    priority = prior;
  }

  /**
   * Runs actual computation.
   */
  @Override
  public void run() {
    operator.onNext(inputs);
  }
}
