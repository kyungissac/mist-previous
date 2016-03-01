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

import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.task.OperatorChain;
import edu.snu.mist.task.PhysicalPlan;
import edu.snu.mist.task.common.OutputEmitter;
import edu.snu.mist.task.sources.SourceInput;

/**
 * QueryManager is responsible for loading queries from storage to memory and unloading queries from memory to storage.
 * There are design choices to implement QueryManager:
 * - How to load queries from storage to memory?
 * - When to load queries from storage to memory?
 * - Which queries do we load from storage to memory?
 * - How to unload queries from memory to storage?
 * - When to unload queries from memory to storage?
 * - Which queries do we unload from memory to storage?
 *
 */
public interface QueryManager extends AutoCloseable, MemoryListener, OutputEmitter<SourceInput> {

  void registerQuery(String queryId,
                     PhysicalPlan<OperatorChain> physicalPlan,
                     LogicalPlan serializedLogicalPlan);

  void unregisterQuery(String queryId);
}