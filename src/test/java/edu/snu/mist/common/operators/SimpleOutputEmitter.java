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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.OutputEmitter;

import java.util.List;

/**
 * Simple output emitter which adds events to the list.
 */
class SimpleOutputEmitter implements OutputEmitter {
  private final List<MistEvent> list;

  SimpleOutputEmitter(final List<MistEvent> list) {
    this.list = list;
  }

  @Override
  public void emitData(final MistDataEvent data) {
    list.add(data);
  }
  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    list.add(watermark);
  }
}