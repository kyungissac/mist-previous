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

import org.apache.avro.ipc.Server;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.task.TaskMessage;
import org.apache.reef.task.TaskMessageSource;
import org.apache.reef.util.Optional;
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A runtime engine running mist queries.
 * The actual query submission logic is performed by QuerySubmitter.
 */
public final class MistTask implements Task, TaskMessageSource {
  private static final Logger LOG = Logger.getLogger(MistTask.class.getName());

  /**
   * A count down latch for sleeping and terminating this task.
   */
  private final CountDownLatch countDownLatch;

  /**
   * Codec to serialize/deserialize counter values for the updates.
   */
  private final ObjectSerializableCodec<Integer> codecInt = new ObjectSerializableCodec<>();

  /**
   * Current value of the counter.
   */
  private int counter = 0;

  /**
   * Default constructor of MistTask.
   * @param server rpc server for receiving queries
   * @throws InjectionException
   */
  @Inject
  private MistTask(final Server server) throws InjectionException {
    this.countDownLatch = new CountDownLatch(1);
  }


  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "MistTask is started");
    countDownLatch.await();
    return new byte[0];
  }

  /**
   * Update driver on current state of the task.
   *
   * @return serialized version of the counter.
   */
  @Override
  public synchronized Optional<TaskMessage> getMessage() {
    LOG.log(Level.INFO, "Message from Task {0} to the Driver: counter: {1}",
            new Object[]{this, this.counter});
    return Optional.of(TaskMessage.from(MistTask.class.getName(), this.codecInt.encode(this.counter)));
  }
}
