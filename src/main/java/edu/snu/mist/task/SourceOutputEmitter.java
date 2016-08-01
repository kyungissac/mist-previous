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
import edu.snu.mist.task.common.MistEvent;
import edu.snu.mist.task.common.MistWatermarkEvent;
import edu.snu.mist.task.common.OutputEmitter;

import java.util.Map;

/**
 * This emitter emits the outputs to the next PartitionedQueries that get inputs from the sources.
 * It always submits jobs to MistExecutors.
 *  @param <I>
 */
final class SourceOutputEmitter<I> implements OutputEmitter {

  /**
   * Next PartitionedQueries.
   */
  private final Map<PartitionedQuery, MistEvent.Direction> nextPartitionedQueries;

  public SourceOutputEmitter(final Map<PartitionedQuery, MistEvent.Direction> nextPartitionedQueries) {
    this.nextPartitionedQueries = nextPartitionedQueries;
  }

  @Override
  public void emitData(final MistDataEvent data) {
    if (nextPartitionedQueries.size() == 1) {
      for (final Map.Entry<PartitionedQuery, MistEvent.Direction> nextQuery :
          nextPartitionedQueries.entrySet()) {
        final MistEvent.Direction direction = nextQuery.getValue();
        nextQuery.getKey().addNextEvent(data, direction);
      }
    } else {
      for (final Map.Entry<PartitionedQuery, MistEvent.Direction> nextQuery :
          nextPartitionedQueries.entrySet()) {
        final MistEvent.Direction direction = nextQuery.getValue();
        final MistDataEvent event = new MistDataEvent(data.getValue(), data.getTimestamp());
        nextQuery.getKey().addNextEvent(event, direction);
      }
    }
  }

  @Override
  public void emitWatermark(final MistWatermarkEvent watermark) {
    for (final Map.Entry<PartitionedQuery, MistEvent.Direction> nextQuery :
        nextPartitionedQueries.entrySet()) {
      final MistEvent.Direction direction = nextQuery.getValue();
      nextQuery.getKey().addNextEvent(watermark, direction);
    }
  }
}
