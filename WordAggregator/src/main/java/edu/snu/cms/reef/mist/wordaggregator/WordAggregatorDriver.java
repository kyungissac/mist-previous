/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.cms.reef.mist.wordaggregator;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.io.network.naming.NameServerParameters;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for the WordCounter Application.
 */
@Unit
public final class WordAggregatorDriver {

  private static final Logger LOG = Logger.getLogger(WordAggregatorDriver.class.getName());
  private final EvaluatorRequestor requestor;
  private final String receiverName;
  //private final NameServer nameServer;

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  private WordAggregatorDriver(final EvaluatorRequestor requestor) throws UnknownHostException, InjectionException {
    this.requestor = requestor;
    LOG.log(Level.FINE, "Instantiated 'WordAggregatorDriver'");
    Injector injector = Tang.Factory.getTang().newInjector();
    //injector.bindVolatileParameter(NameServerParameters.NameServerAddr.class, "192.168.0.64"); 
    //injector.bindVolatileParameter(NameServerParameters.NameServerPort.class, 11780);
    //this.nameServer = injector.getInstance(NameServer.class);
    this.receiverName = "receiver";
  }

  /**
   * Handles the StartTime event: Request Evaluators.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      WordAggregatorDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  /**
   * Handles AllocatedEvaluator.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.FINE, "Evaluator allocated");
      final Configuration contextConf;
      contextConf = ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, "context_0")
        .build();
      allocatedEvaluator.submitContext(contextConf);
    }
  }

  /**
   * Handles Activated Context: Submit the WordAggregatorTask.
   */
  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public synchronized void onNext(final ActiveContext context) {
        final Configuration partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "receiver_task")
            .set(TaskConfiguration.TASK, WordAggregatorTask.class)
            .build();
        final Configuration netConf = NameResolverConfiguration.CONF
            .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, "master")
            .set(NameResolverConfiguration.NAME_SERVICE_PORT, 11780)
            .build();
        final JavaConfigurationBuilder taskConfBuilder =
            Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf, netConf);
        taskConfBuilder.bindNamedParameter(WordAggregatorTask.ReceiverName.class, receiverName);
        final Configuration taskConf = taskConfBuilder.build();
        context.submitTask(taskConf);
    }
  }

  /**
   * Handles Running Tasks.
   */
  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {

    }
  }
}
