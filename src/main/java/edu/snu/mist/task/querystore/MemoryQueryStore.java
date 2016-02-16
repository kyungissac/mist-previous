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
package edu.snu.mist.task.querystore;

import org.apache.reef.io.Tuple;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This is a basic query store which stores query info/states into hashmap.
 */
public final class MemoryQueryStore implements QueryStore {


  private final ConcurrentMap<String, byte[]> stateMap;

  private final ConcurrentMap<String, byte[]> infoMap;

  @Inject
  private MemoryQueryStore() {
    this.stateMap = new ConcurrentHashMap<>();
    this.infoMap = new ConcurrentHashMap<>();
  }


  @Override
  public void storeState(final String queryId,
                         final Tuple<String, byte[]> tuple,
                         final EventHandler<Tuple<String, byte[]>> callback) {
    stateMap.put(queryId, tuple.getValue());
    callback.onNext(tuple);
  }

  @Override
  public void storeInfo(final String queryId,
                        final Tuple<String, byte[]> tuple,
                        final EventHandler<Tuple<String, byte[]>> callback) {
    infoMap.put(queryId, tuple.getValue());
    callback.onNext(tuple);
  }

  @Override
  public void getState(final String queryId, final EventHandler<byte[]> callback) {
    callback.onNext(stateMap.get(queryId));
  }

  @Override
  public void getInfo(final String queryId, final EventHandler<byte[]> callback) {
    callback.onNext(infoMap.get(queryId));
  }

  @Override
  public void deleteQuery(final String queryId, final EventHandler<String> callback) {
    infoMap.remove(queryId);
    stateMap.remove(queryId);
    callback.onNext(queryId);
  }
}
