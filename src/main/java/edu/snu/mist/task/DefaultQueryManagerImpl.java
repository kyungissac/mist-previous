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

import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.task.executor.MistExecutor;
import edu.snu.mist.task.parameters.GracePeriod;
import edu.snu.mist.task.querystore.QueryStore;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

final class DefaultQueryManagerImpl implements QueryManager {

  private final ConcurrentMap<String, QueryContent> queryInfoMap;

  private final QueryStore queryStore;

  private final EventHandler<String> deleteCallback;

  private final AtomicLong lastCheckpoint;

  private final long gracePeriod;

  private final ScheduledExecutorService scheduledExecutorService;

  @Inject
  private DefaultQueryManagerImpl(final QueryStore queryStore,
                                  @Parameter(GracePeriod.class) final long gracePeriod) {
    this.queryInfoMap = new ConcurrentHashMap<>();
    this.queryStore = queryStore;
    this.deleteCallback = deletedId -> {};
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        // checkpoint and unload states/info from memory.

      }
    }, gracePeriod, gracePeriod, TimeUnit.MILLISECONDS);
    this.lastCheckpoint = new AtomicLong(System.currentTimeMillis());
    this.gracePeriod = gracePeriod;
  }

  @Override
  public void registerQuery(final String queryId,
                            final PhysicalPlan<OperatorChain> physicalPlan,
                            final LogicalPlan logicalPlan) {
    final QueryContent queryContent = new DefaultQueryContentImpl(queryId,
        physicalPlan.getSourceMap(), physicalPlan.getOperators(), physicalPlan.getSinkMap(), logicalPlan);
    queryInfoMap.put(queryId, queryContent);
    // TODO[MIST-#]: Deserialize physical plan and store query info and state to QueryStore.
  }

  private String logicalPlanToString(final LogicalPlan logicalPlan) {
    final DatumWriter<LogicalPlan> datumWriter = new SpecificDatumWriter<>(LogicalPlan.class);
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(logicalPlan.getSchema(), out);
      datumWriter.write(logicalPlan, encoder);
      encoder.flush();
      out.close();
      return out.toString();
    } catch (final IOException ex) {
      throw new RuntimeException("Unable to serialize logicalPlan", ex);
    }
  }

  private LogicalPlan logicalPlanFromString(final String serializedLogicalPlan) {
    try {
      final Decoder decoder =
          DecoderFactory.get().jsonDecoder(LogicalPlan.getClassSchema(), serializedLogicalPlan);
      final SpecificDatumReader<LogicalPlan> reader = new SpecificDatumReader<>(LogicalPlan.class);
      return reader.read(null, decoder);
    } catch (final IOException ex) {
      throw new RuntimeException("Unable to deserialize logical plan", ex);
    }
  }

  @Override
  public void unregisterQuery(final String queryId) {
    queryInfoMap.remove(queryId);
    queryStore.deleteQuery(queryId, deleteCallback);
  }

  @Override
  public void close() throws Exception {

  }

  private void forwardInput(final Set<OperatorChain> nextOperators, final Object input) {
    for (final OperatorChain nextOp : nextOperators) {
      final MistExecutor executor = nextOp.getExecutor();
      final OperatorChainJob operatorChainJob = new DefaultOperatorChainJob(nextOp, input);
      // Always submits a job to the MistExecutor when inputs are received.
      executor.submit(operatorChainJob);
    }
  }

  @Override
  public void emit(final SourceInput sourceInput) {
    final QueryContent queryContent = queryInfoMap.get(sourceInput.getQueryId());
    final Queue queue = queryContent.getQueue();
    final Object input = sourceInput.getInput();
    final Set<OperatorChain> nextOps = queryContent.getSourceMap().get(sourceInput.getSrc());
    final long currTime = System.currentTimeMillis();
    queryContent.setLatestActiveTime(currTime);

    switch (queryContent.getQueryStatus()) {
      case ACTIVE:
        if (!queue.isEmpty()) {
          // if query is active but queue is not empty, add the input to the queue.
          synchronized (queue) {
            queue.add(input);
          }
        } else {
          forwardInput(nextOps, input);
        }
        break;
      case PARTIALLY_ACTIVE:
        // Load states of StatefulOperators.
        // After loading the states, it should set the query status to ACTIVE
        // and forwards the inputs in the queue to next operators.
        queue.add(input);
        queryContent.setQueryStatus(QueryContent.QueryStatus.ACTIVE);
        // TODO[MIST-#]: Load states of a query.
        break;
      case INACTIVE:
        // Load states and info of the query.
        // After loading the states and info, it should set the query status to ACTIVE
        // and forwards the inputs in the queue to next operators.
        queue.add(input);
        queryContent.setQueryStatus(QueryContent.QueryStatus.ACTIVE);
        // TODO[MIST-#]: Load info of a query.
        break;
      default:
        throw new RuntimeException("Invalid query status");
    }
  }
}
