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
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.OperatorId;
import edu.snu.mist.common.parameters.SerializedUdf;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Map operator which maps input.
 * @param <I> input type
 * @param <I> output type
 */
public final class MapOperator<I, O> extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(MapOperator.class.getName());

  /**
   * Map function.
   */
  private final MISTFunction<I, O> mapFunc;

  @Inject
  private MapOperator(
      @Parameter(OperatorId.class) final String operatorId,
      @Parameter(SerializedUdf.class) final String serializedObject,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    this(operatorId, SerializeUtils.deserializeFromString(serializedObject, classLoader));
  }

  @Inject
  public MapOperator(@Parameter(OperatorId.class) final String operatorId,
                     final MISTFunction<I, O> mapFunc) {
    super(operatorId);
    this.mapFunc = mapFunc;
  }

  /**
   * Maps the input to the output.
   */
  @Override
  public void processLeftData(final MistDataEvent data) {
    final O output = mapFunc.apply((I)data.getValue());
    LOG.log(Level.FINE, "{0} maps {1} to {2}", new Object[]{MapOperator.class, data, output});
    data.setValue(output);
    outputEmitter.emitData(data);
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent watermark) {
    outputEmitter.emitWatermark(watermark);
  }

}