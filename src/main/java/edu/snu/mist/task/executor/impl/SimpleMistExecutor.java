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
import edu.snu.mist.task.executor.MistExecutor;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.WakeParameters;

import javax.inject.Inject;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Simple mist executor which uses ThreadPoolExecutor and a priority queue for scheduling.
 */
public final class SimpleMistExecutor implements MistExecutor {
  private static final Logger LOG = Logger.getLogger(SimpleMistExecutor.class.getName());

  /**
   * An identifier of the mist executor.
   */
  private final Identifier identifier;

  /**
   * A thread pool executor.
   */
  private final ThreadPoolExecutor tpExecutor;

  /**
   * A priority blocking queue for scheduling.
   */
  private final PriorityBlockingQueue<Runnable> queue;

  /**
   * A simple executor task's scheduler.
   */
  private final SimpleExecutorScheduler scheduler;

  /**
   * A flag if the executor is closed.
   */
  private final AtomicBoolean closed = new AtomicBoolean(false);

  /**
   * A shutdown time.
   */
  private final long shutdownTimeout = WakeParameters.EXECUTOR_SHUTDOWN_TIMEOUT;

  @Inject
  private SimpleMistExecutor(final Identifier identifier,
                             final SimpleExecutorScheduler scheduler) {
    this.identifier = identifier;
    this.queue = new PriorityBlockingQueue<>(100, scheduler);
    this.tpExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, queue);
    this.scheduler = scheduler;
  }

  /**
   * Runs an executor task according to the priority.
   * @param executorTask an executor task
   */
  @Override
  public void onNext(final ExecutorTask executorTask) {
    // set executor task's priority
    scheduler.setPriorityOfExecutorTask(executorTask);
    tpExecutor.submit(executorTask);
  }

  /**
   * Gets current load of the executor.
   * @return
   */
  @Override
  public int getCurrentLoad() {
    return queue.size();
  }

  /**
   * Closes the executor.
   * @throws Exception
   */
  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      tpExecutor.shutdown();
      if (!tpExecutor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS)) {
        LOG.log(Level.WARNING, "Executor did not terminate in " + shutdownTimeout + "ms.");
        final List<Runnable> droppedRunnables = tpExecutor.shutdownNow();
        LOG.log(Level.WARNING, "Executor dropped " + droppedRunnables.size() + " tasks.");
      }
    }
  }

  /**
   * SimpleExecutorScheduler for scheduling executor tasks.
   */
  public interface SimpleExecutorScheduler extends Comparator<Runnable> {

    /**
     * Sets the priority of the executor task for scheduling.
     * @param executorTask an executor task
     * @param <I> input type
     */
    <I> void setPriorityOfExecutorTask(final ExecutorTask<I> executorTask);

    /**
     * Compares two different executor tasks and decides which task has high priority.
     * @param t1 a task in front of t2
     * @param t2 a task behind t1
     * @return -1, 0: maintain the order, 1: swap the order
     */
    @Override
    int compare(Runnable t1, Runnable t2);
  }
}
