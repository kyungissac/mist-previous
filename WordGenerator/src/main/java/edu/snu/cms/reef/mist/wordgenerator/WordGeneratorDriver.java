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
package edu.snu.cms.reef.mist.wordgenerator;

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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for the WordGenerator Application.
 */
@Unit
public final class WordGeneratorDriver {

  private static final Logger LOG = Logger.getLogger(WordGeneratorDriver.class.getName());
  private final EvaluatorRequestor requestor;
  private final NameServer nameServer;
  private final String senderName;
  private final String driverHostAddress;
  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  private WordGeneratorDriver(final EvaluatorRequestor requestor) throws UnknownHostException, InjectionException {
    this.requestor = requestor;
    LOG.log(Level.FINE, "Instantiated 'WordGeneratorDriver'");
    Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileParameter(NameServerParameters.NameServerPort.class, 11780);
    this.nameServer = injector.getInstance(NameServer.class);
    this.senderName = "sender";
    this.driverHostAddress = Inet4Address.getLocalHost().getHostAddress();
  }

  /**
   * Handles the StartTime event: Request Evaluators.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      WordGeneratorDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
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
          .set(ContextConfiguration.IDENTIFIER, "context")
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
          .set(TaskConfiguration.IDENTIFIER, "sender_task")
          .set(TaskConfiguration.TASK, WordGeneratorTask.class)
          .build();
      final Configuration netConf = NameResolverConfiguration.CONF
          .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, driverHostAddress)
          .set(NameResolverConfiguration.NAME_SERVICE_PORT, WordGeneratorDriver.this.nameServer.getPort())
          .build();
      final JavaConfigurationBuilder taskConfBuilder =
          Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf, netConf);
      taskConfBuilder.bindNamedParameter(WordGeneratorTask.SenderName.class, senderName);
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
