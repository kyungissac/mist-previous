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
package edu.snu.mist.task.operator;

import edu.snu.mist.task.executor.MistExecutor;
import org.apache.reef.wake.Identifier;

import java.util.LinkedList;
import java.util.List;

/**
 * This is a base operator which implements basic function of the operator.
 * @param <I> input
 * @param <O> output
 */
public abstract class BaseOperator<I, O> implements Operator<I, O> {
  /**
   * Downstream operators which receives outputs of this operator as inputs.
   */
  protected final List<Operator<O, ?>> downstreamOperators;

  /**
   * An assigned executor for this operator.
   */
  protected MistExecutor executor;

  /**
   * An identifier of the operator.
   */
  protected final Identifier identifier;

  public BaseOperator(final Identifier identifier) {
    this.downstreamOperators = new LinkedList<>();
    this.identifier = identifier;
  }

  @Override
  public MistExecutor getExecutor() {
    return executor;
  }

  @Override
  public void assignExecutor(final MistExecutor exec) {
    executor = exec;
  }

  @Override
  public void addDownstreamOperator(final Operator<O, ?> operator) {
    downstreamOperators.add(operator);
  }

  @Override
  public void addDownstreamOperators(final List<Operator<O, ?>> operators) {
    downstreamOperators.addAll(operators);
  }

  @Override
  public void removeDownstreamOperator(final Operator<O, ?> operator) {
    downstreamOperators.remove(operator);
  }

  @Override
  public void removeDownstreamOperators(final List<Operator<O, ?>> operators) {
    downstreamOperators.removeAll(operators);
  }

  @Override
  public List<Operator<O, ?>> getDownstreamOperators() {
    return downstreamOperators;
  }
}
