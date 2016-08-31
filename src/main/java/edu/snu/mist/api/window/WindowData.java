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
package edu.snu.mist.api.window;

import java.util.Collection;

/**
 * This interface represents the result data of windowing operation.
 * It contains the result data collection, start and end information.
 * The start and end may represents time or count.
 * @param <T> the type of data in this window
 */
public interface WindowData<T> {

  /**
   * @return the result data collection of window
   */
  Collection<T> getDataCollection();

  /**
   * @return the start time or count
   */
  long getStart();

  /**
   * @return the end time or count
   */
  long getEnd();
}
