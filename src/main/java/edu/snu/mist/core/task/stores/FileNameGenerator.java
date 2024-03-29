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
package edu.snu.mist.core.task.stores;


import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This generates file names for stored jar files.
 */
@DefaultImplementation(SequenceFileNameGenerator.class)
interface FileNameGenerator {

  /**
   * Generate a file name.
   * @return a file name
   */
  String generate();
}

