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
package edu.snu.mist.task.querymanager;

import org.apache.reef.io.Tuple;
import org.apache.reef.wake.EventHandler;


public interface QueryStore {

  void storeState(String queryId, String state,
                         EventHandler<Tuple<String, String>> callback);

  void storeLogicalPlan(String queryId, String logicalPlan,
                               EventHandler<Tuple<String, String>> callback);

  void getState(String queryId, EventHandler<Tuple<String, String>> callback);

  void getInfo(String queryId, EventHandler<Tuple<String, String>> callback);

  void deleteQuery(String queryId, EventHandler<String> callback);
}