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

import edu.snu.mist.task.common.MistDataEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;
import edu.snu.mist.task.common.OutputEmitter;
import edu.snu.mist.task.sinks.Sink;

import java.util.Set;

/**
 * This emitter sends the outputs to Sinks.
 */
final class SinkEmitter implements OutputEmitter {

  /**
   * Next Sinks.
   */
  private final Set<Sink> sinks;

  public SinkEmitter(final Set<Sink> sinks) {
    this.sinks = sinks;
  }

  @Override
  public void emitData(final MistDataEvent data) {
    for (final Sink sink : sinks) {
      sink.handle(data.getValue());
    }
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    // do nothing because sink doesn't have to handle watermark.
  }
}
