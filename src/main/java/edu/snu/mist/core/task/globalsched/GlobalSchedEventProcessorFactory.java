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
package edu.snu.mist.core.task.globalsched;

import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.eventProcessors.EventProcessorFactory;

import javax.inject.Inject;

/**
 * The factory class of GlobalSchedEventPrcoessor.
 */
public final class GlobalSchedEventProcessorFactory implements EventProcessorFactory {

  /**
   * The timeslice calculator.
   */
  private final GroupTimesliceCalculator timesliceCalculator;

  /**
   * Selector of the executable group.
   */
  private final NextGroupSelector nextGroupSelector;

  @Inject
  private GlobalSchedEventProcessorFactory(final GroupTimesliceCalculator timesliceCalculator,
                                           final NextGroupSelector nextGroupSelector) {
    super();
    this.timesliceCalculator = timesliceCalculator;
    this.nextGroupSelector = nextGroupSelector;
  }

  @Override
  public EventProcessor newEventProcessor() {
    return new GlobalSchedEventProcessor(timesliceCalculator, nextGroupSelector);
  }
}