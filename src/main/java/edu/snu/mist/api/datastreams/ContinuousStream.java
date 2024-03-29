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
package edu.snu.mist.api.datastreams;

import edu.snu.mist.common.functions.*;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.WindowInformation;
import org.apache.reef.tang.Configuration;

import java.util.List;
import java.util.Map;

/**
 * Continuous Stream is a normal Stream used inside MIST. It emits one stream data (typed T) for one time.
 * It should be distinguished from WindowedStream.
 */
public interface ContinuousStream<T> extends MISTStream<T> {

  /**
   * Applies map operation to the current stream and creates a new stream.
   * @param mapFunc the function used for the transformation provided by a user.
   * @param <OUT> the type of newly created stream output
   * @return new transformed stream after applying the operation
   */
  <OUT> ContinuousStream<OUT> map(MISTFunction<T, OUT> mapFunc);

  /**
   * Applies map operation to the current stream and creates a new stream.
   * @param clazz the class of map function used for the transformation
   * @param funcConf a configuration to instantiate the map function from the provided class
   * @param <OUT> the type of newly created stream output
   * @return new transformed stream after applying the operation
   */
  <OUT> ContinuousStream<OUT> map(Class<? extends MISTFunction<T, OUT>> clazz,
                                  Configuration funcConf);

  /**
   * Applies flatMap operation to the current stream and creates a new stream.
   * @param flatMapFunc the function used for the transformation provided by a user.
   * @param <OUT> the type of newly created stream output
   * @return new transformed stream after applying the operation
   */
  <OUT> ContinuousStream<OUT> flatMap(MISTFunction<T, List<OUT>> flatMapFunc);

  /**
   * Applies flatMap operation to the current stream and creates a new stream.
   * @param clazz the class of flat-map function used for the transformation.
   * @param funcConf a configuration to instantiate the flat-map function from the provided class
   * @param <OUT> the type of newly created stream output
   * @return new transformed stream after applying the operation
   */
  <OUT> ContinuousStream<OUT> flatMap(Class<? extends MISTFunction<T, List<OUT>>> clazz,
                                      Configuration funcConf);

  /**
   * Applies filter operation to the current stream and creates a new stream.
   * @param filterFunc the function used for the transformation provided by a user.
   * @return new transformed stream after applying the operation
   */
  ContinuousStream<T> filter(MISTPredicate<T> filterFunc);

  /**
   * Applies filter operation to the current stream and creates a new stream.
   * @param clazz the class of filter function used for the transformation.
   * @param funcConf a configuration to instantiate the filter function from the provided class
   * @return new transformed stream after applying the operation
   */
  ContinuousStream<T> filter(Class<? extends MISTPredicate<T>> clazz,
                             Configuration funcConf);

  /**
   * Applies reduceByKey operation to the current stream.
   * @param keyFieldIndex the field index of key field
   * @param keyType the type of key. This parameter is used for type inference and dynamic type checking
   * @param reduceFunc function used for reduce operation
   * @param <K> the type of key in resulting stream
   * @param <V> the type of value in resulting stream
   * @return new transformed stream after applying the operation
   */
  <K, V> ContinuousStream<Map<K, V>> reduceByKey(
      int keyFieldIndex, Class<K> keyType, MISTBiFunction<V, V, V> reduceFunc);

  /**
   * Applies reduceByKey operation to the current stream.
   * @param keyFieldIndex the field index of key field
   * @param keyType the type of key. This parameter is used for type inference and dynamic type checking
   * @param clazz the class of reduce by key function used for the transformation.
   * @param funcConf a configuration to instantiate the reduce by key function from the provided class
   * @param <K> the type of key in resulting stream
   * @param <V> the type of value in resulting stream
   * @return new transformed stream after applying the operation
   */
  <K, V> ContinuousStream<Map<K, V>> reduceByKey(int keyFieldIndex, Class<K> keyType,
                                                 Class<? extends MISTBiFunction<V, V, V>> clazz,
                                                 Configuration funcConf);
  /**
   * Applies user-defined stateful operator to the current stream.
   * This stream will produce outputs on every stream input.
   * @param applyStatefulFunction the user-defined ApplyStatefulFunction
   * @param <OUT> the type of stream output
   * @return new transformed stream after applying the user-defined stateful operation
   */
  <OUT> ContinuousStream<OUT> applyStateful(ApplyStatefulFunction<T, OUT> applyStatefulFunction);

  /**
   * Applies user-defined stateful operator to the current stream.
   * This stream will produce outputs on every stream input.
   * @param clazz the class of apply stateful function used for the transformation.
   * @param funcConf a configuration to instantiate the apply stateful function from the provided class
   * @param <OUT> the type of stream output
   * @return new transformed stream after applying the user-defined stateful operation
   */
  <OUT> ContinuousStream<OUT> applyStateful(Class<? extends ApplyStatefulFunction<T, OUT>> clazz,
                                            Configuration funcConf);
  /**
   * Applies union operation to the current stream and input continuous stream passed as a parameter.
   * Both two streams for union should be continuous stream type.
   * @param inputStream the stream to be unified with this stream
   * @return new unified stream after applying type-checking
   */
  ContinuousStream<T> union(ContinuousStream<T> inputStream);

  /**
   * Creates a new WindowsStream according to the WindowInformation.
   * @param windowInfo the WindowInformation contains some information used during windowing operation
   * @return new windowed stream after applying the windowing operation
   */
  WindowedStream<T> window(WindowInformation windowInfo);

  /**
   * Joins current stream with the input stream.
   * Two streams are windowed according to the WindowInfo and joined within the window.
   * @param inputStream the stream to be joined with this stream
   * @param joinBiPredicate the function that decides to join a pair of inputs in two streams
   * @param windowInfo the windowing information for joining two streams
   * @param <U> the data type of the input stream to be joined with this stream
   * @return new windowed and joined stream
   */
  <U> WindowedStream<Tuple2<T, U>> join(ContinuousStream<U> inputStream,
                                        MISTBiPredicate<T, U> joinBiPredicate,
                                        WindowInformation windowInfo);

  /**
   * Joins current stream with the input stream.
   * Two streams are windowed according to the WindowInfo and joined within the window.
   * @param inputStream the stream to be joined with this stream
   * @param clazz the class of join function used for the transformation.
   * @param funcConf a configuration to instantiate the join function from the provided class
   * @param windowInfo the windowing information for joining two streams
   * @param <U> the data type of the input stream to be joined with this stream
   * @return new windowed and joined stream
   */
  <U> WindowedStream<Tuple2<T, U>> join(ContinuousStream<U> inputStream,
                                        Class<? extends MISTBiPredicate<T, U>> clazz,
                                        Configuration funcConf,
                                        WindowInformation windowInfo);

  /**
   * Push the text stream to th socket server.
   * @param serverAddr socket server address
   * @param serverPort socket server port
   * @return sink stream
   */
  MISTStream<String> textSocketOutput(String serverAddr, int serverPort);
}
