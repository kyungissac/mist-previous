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

import com.google.common.collect.ImmutableList;
import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.executor.impl.DefaultExecutorTask;
import edu.snu.mist.task.operator.operation.immediate.ImmediateOperation;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Immediate operator transforms inputs by doing computation
 * and immediately pushes the results to downstream operators.
 * @param <I> input
 * @param <O> output
 */
final class ImmediateOperator<I, O> extends BaseOperator<I, O> {
  private static final Logger LOG = Logger.getLogger(ImmediateOperator.class.getName());

  /**
   * Actual computation of this operator.
   */
  private final ImmediateOperation<I, O> operation;

  @Inject
  private ImmediateOperator(final ImmediateOperation<I, O> operation,
                            final Identifier identifier) {
    super(identifier);
    this.operation = operation;
  }

  /**
   * It receives inputs, performs computation,
   * and forwards the produced outputs to downstream operators.
   *
   * If the downstream operator's executor is different from current one,
   * then executes downstream computation as a job of the downstream executor.
   * Else, just executes downstream computation by doing function call.
   * @param inputs inputs.
   */
  @Override
  public void onNext(final ImmutableList<I> inputs) {
    final ImmutableList<O> outputs = operation.compute(inputs);
    LOG.log(Level.FINE, "{0} computes {1} to {2}", new Object[] {identifier, inputs, outputs});
    for (final Operator<O, ?> downstreamOp : downstreamOperators) {
      final MistExecutor dsExecutor = downstreamOp.getExecutor();
      if (dsExecutor.equals(executor)) {
        // just do function call instead of context switching.
        downstreamOp.onNext(outputs);
      } else {
        // submit as a job in order to do execute the operation in another thread.
        dsExecutor.onNext(new DefaultExecutorTask<>(downstreamOp, outputs));
      }
    }
  }
}
