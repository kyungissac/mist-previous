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

import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.DriverLauncher;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.runtime.yarn.client.YarnClientConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.exceptions.BindException;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Client for WordCounter example.
 */
public final class WordGeneratorClient {
  private static final Logger LOG = Logger.getLogger(WordGeneratorClient.class.getName());

  /**
   * Local runtime configuration.
   * @return the configuration of the runtime
   */
  private static Configuration getRuntimeConfiguration() {
    return YarnClientConfiguration.CONF.build();
  }
  /**
   * @return the configuration of the WordGeneratorClient driver.
   */
  private static Configuration getDriverConfiguration() {
    return DriverConfiguration.CONF
        .set(DriverConfiguration.GLOBAL_LIBRARIES,
            WordGeneratorClient.class.getProtectionDomain().getCodeSource().getLocation().getFile())
        .set(DriverConfiguration.DRIVER_IDENTIFIER, "WordGeneratorDriver")
        .set(DriverConfiguration.ON_DRIVER_STARTED, WordGeneratorDriver.StartHandler.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, WordGeneratorDriver.EvaluatorAllocatedHandler.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, WordGeneratorDriver.ActiveContextHandler.class)
        .set(DriverConfiguration.ON_TASK_RUNNING, WordGeneratorDriver.RunningTaskHandler.class)
        .build();
  }

  /**
   * Start WordGenerator job.
   *
   * @param args command line parameters.
   * @throws BindException      configuration error.
   * @throws InjectionException configuration error.
   */
  public static void main(final String[] args) throws BindException, InjectionException {
    final Configuration runtimeConf = getRuntimeConfiguration();
    final Configuration driverConf = getDriverConfiguration();

    final LauncherStatus status = DriverLauncher
        .getLauncher(runtimeConf)
        .run(driverConf);
    LOG.log(Level.INFO, "REEF job completed: {0}", status);
  }

  /**
   * Empty private constructor to prohibit instantiation of utility class.
   */
  private WordGeneratorClient() {
  }
}
