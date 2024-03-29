/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.api.datastreams.configurations;

import org.apache.reef.tang.Configuration;

/**
 * The class represents watermark configuration.
 * This is just a wrapper class that holds the configuration of Tang.
 */
public final class WatermarkConfiguration {

  private final Configuration conf;
  WatermarkConfiguration(final Configuration conf) {
    this.conf = conf;
  }

  /**
   * Get the configuration.
   * @return configuration
   */
  public Configuration getConfiguration() {
    return conf;
  }
}