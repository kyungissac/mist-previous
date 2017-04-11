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
package edu.snu.mist.core.task;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.graph.AdjacentListDAG;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.FilterOperator;
import edu.snu.mist.common.parameters.GroupId;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.TestParameters;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Test whether ImmediateQueryMergingStarter merges and starts the submitted queries correctly.
 */
public final class ImmediateQueryMergingStarterTest {

  /**
   * Execution dag generator.
   */
  private DagGenerator dagGenerator;

  /**
   * Immediate query merging starter.
   */
  private ImmediateQueryMergingStarter queryMerger;

  /**
   * A variable for creating configurations.
   */
  private int confCount = 0;

  /**
   * A variable for creating identifiers.
   */
  private int idCount = 0;

  @Before
  public void setUp() throws InjectionException, IOException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    dagGenerator = Tang.Factory.getTang().newInjector().getInstance(DagGenerator.class);
    queryMerger = injector.getInstance(ImmediateQueryMergingStarter.class);
  }

  @After
  public void tearDown() throws IOException {
    idCount = 0;
    confCount = 0;
  }

  /**
   * Get a test source with the configuration.
   * @param conf source configuration
   * @return test source
   */
  private TestSource generateSource(final String conf) {
    return new TestSource(generateId(), conf);
  }

  /**
   * Get a simple operator chain that has a filter operator.
   * @param conf configuration of the operator
   * @return operator chain
   */
  private OperatorChain generateFilterOperatorChain(final String conf,
                                                    final MISTPredicate<String> predicate) {
    final OperatorChain operatorChain = new DefaultOperatorChainImpl();
    final PhysicalOperator filterOp = new DefaultPhysicalOperatorImpl(generateId(),
        conf, new FilterOperator<>(predicate), operatorChain);
    operatorChain.insertToHead(filterOp);
    return operatorChain;
  }

  /**
   * Get a sink that stores the outputs to the list.
   * @param conf configuration of the sink
   * @param result list for storing outputs
   * @return sink
   */
  private PhysicalSink<String> generateSink(final String conf,
                                            final List<String> result) {
    return new PhysicalSinkImpl<>(generateId(), conf, new TestSink<>(result));
  }

  /**
   * Generate a simple query that has the following structure: src -> operator chain -> sink.
   * @param source source
   * @param operatorChain operator chain
   * @param sink sink
   * @return dag
   */
  private DAG<ExecutionVertex, MISTEdge> generateSimpleQuery(final TestSource source,
                                                             final OperatorChain operatorChain,
                                                             final PhysicalSink<String> sink) {
    // Create DAG
    final DAG<ExecutionVertex, MISTEdge> dag = new AdjacentListDAG<>();
    dag.addVertex(source);
    dag.addVertex(operatorChain);
    dag.addVertex(sink);
    dag.addEdge(source, operatorChain, new MISTEdge(Direction.LEFT));
    dag.addEdge(operatorChain, sink, new MISTEdge(Direction.LEFT));
    return dag;
  }

  /**
   * Generate an identifier.
   * @return identifier
   */
  private String generateId() {
    idCount += 1;
    return Integer.toString(idCount);
  }

  /**
   * Generate a configuration for source, operator, and sink.
   * @return configuration
   */
  private String generateConf() {
    confCount += 1;
    return Integer.toString(confCount);
  }

  /**
   * Generate a group info instance that has the group id.
   * @param groupId group id
   * @return group info
   * @throws InjectionException
   */
  private GroupInfo generateGroupInfo(final String groupId) throws InjectionException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindNamedParameter(GroupId.class, groupId);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    return injector.getInstance(GroupInfo.class);
  }

  /**
   * Test cases
   * Case 1. Start a single query
   * Case 2. Two queries have one source but the sources are different.
   * Case 3. Two queries have one same source, and same operator chain
   * Case 4. Two queries have one same, source, but different operator chain
   * Case 5. Two queries have two sources, same two sources, same operator chains
   * Case 6. Two queries have two sources, one same source, one different source
   * Case 7. Three queries - two execution Dags and one submitted Dag
   *  - The submitted query has two same sources with the two execution dags
   */

  /**
   * Case 1: Start a single query.
   * Test if it executes a single query correctly when there are no execution dags that are currently running
   */
  @Test
  public void singleQueryMergingTest() throws InjectionException {
    final List<String> result = new LinkedList<>();
    final String sourceConf = generateConf();
    final TestSource source = generateSource(sourceConf);
    final OperatorChain operatorChain = generateFilterOperatorChain(generateConf(), (s) -> true);
    final PhysicalSink<String> sink = generateSink(generateConf(), result);
    final DAG<ExecutionVertex, MISTEdge> query = generateSimpleQuery(source, operatorChain, sink);
    // Execute the query 1
    final GroupInfo groupInfo = generateGroupInfo(TestParameters.GROUP_ID);
    queryMerger.start(groupInfo, query);
    // Generate events for the query and check if the dag is executed correctly
    final String data1 = "Hello";
    source.send(data1);
    Assert.assertEquals(Arrays.asList(), result);
    Assert.assertEquals(1, operatorChain.numberOfEvents());
    Assert.assertEquals(true, operatorChain.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result);
    Assert.assertEquals(query, groupInfo.getExecutionDags().get(sourceConf));
  }

  /**
   * Case 2: Merging two queries that have different sources.
   */
  @Test
  public void mergingDifferentSourceQueriesOneGroupTest() throws InjectionException {
    // Create a query 1:
    // src1 -> oc1 -> sink1
    final List<String> result1 = new LinkedList<>();
    final TestSource src1 = generateSource(generateConf());
    final OperatorChain operatorChain1 = generateFilterOperatorChain(generateConf(), (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(generateConf(), result1);
    final DAG<ExecutionVertex, MISTEdge> query1 = generateSimpleQuery(src1, operatorChain1, sink1);
    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 is different from that of src1.
    final List<String> result2 = new LinkedList<>();
    final TestSource src2 = generateSource(generateConf());
    final OperatorChain operatorChain2 = generateFilterOperatorChain(generateConf(), (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(generateConf(), result2);
    final DAG<ExecutionVertex, MISTEdge> query2 = generateSimpleQuery(src2, operatorChain2, sink2);

    // Execute two queries
    final GroupInfo groupInfo = generateGroupInfo(TestParameters.GROUP_ID);
    queryMerger.start(groupInfo, query1);
    queryMerger.start(groupInfo, query2);

    // Check
    final ExecutionDags<String> executionDags = groupInfo.getExecutionDags();
    Assert.assertEquals(2, executionDags.size());
    Assert.assertEquals(query1, executionDags.get(src1.getConfiguration()));
    Assert.assertEquals(query2, executionDags.get(src2.getConfiguration()));

    // The query 1 and 2 have different sources, so they should be executed separately
    final String data1 = "Hello";
    src1.send(data1);
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result1);
    Assert.assertEquals(false, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(), result2);

    final String data2 = "World";
    src2.send(data2);
    Assert.assertEquals(true, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data2), result2);
    Assert.assertEquals(false, operatorChain1.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result1);
  }

  /**
   * Case 3: Merging two dags that have same source and operator chain.
   * @throws InjectionException
   */
  @Test
  public void mergingSameSourceAndSameOperatorQueriesOneGroupTest()
      throws InjectionException, IOException, ClassNotFoundException {
    // Create a query 1:
    // src1 -> oc1 -> sink1
    final List<String> result1 = new LinkedList<>();
    final String sourceConf = generateConf();
    final String operatorConf = generateConf();
    final TestSource src1 = generateSource(sourceConf);
    final OperatorChain operatorChain1 = generateFilterOperatorChain(operatorConf, (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(generateConf(), result1);
    final DAG<ExecutionVertex, MISTEdge> query1 = generateSimpleQuery(src1, operatorChain1, sink1);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 and operatorChain2 is same as that of src1 and operatorChain2.
    final List<String> result2 = new LinkedList<>();
    final TestSource src2 = generateSource(sourceConf);
    final OperatorChain operatorChain2 = generateFilterOperatorChain(operatorConf, (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(generateConf(), result2);
    final DAG<ExecutionVertex, MISTEdge> query2 = generateSimpleQuery(src2, operatorChain2, sink2);

    // Execute the query 1
    final GroupInfo groupInfo = generateGroupInfo(TestParameters.GROUP_ID);
    queryMerger.start(groupInfo, query1);

    // The query 1 and 2 should be merged and the following dag should be created:
    // src1 -> oc1 -> sink1
    //             -> sink2
    final DAG<ExecutionVertex, MISTEdge> expectedDag = new AdjacentListDAG<>();
    GraphUtils.copy(query1, expectedDag);
    expectedDag.addVertex(sink2);
    expectedDag.addEdge(operatorChain1, sink2, query2.getEdges(operatorChain2).get(sink2));

    // Execute the query 2
    queryMerger.start(groupInfo, query2);

    final ExecutionDags<String> executionDags = groupInfo.getExecutionDags();
    final DAG<ExecutionVertex, MISTEdge> mergedDag = executionDags.get(sourceConf);
    Assert.assertEquals(1, executionDags.size());
    Assert.assertEquals(expectedDag, mergedDag);

    // Generate events for the merged query and check if the dag is executed correctly
    final String data = "Hello";
    src1.send(data);
    Assert.assertEquals(1, operatorChain1.numberOfEvents());
    Assert.assertEquals(0, operatorChain2.numberOfEvents());
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(false, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data), result1);
    Assert.assertEquals(Arrays.asList(data), result2);
  }

  /**
   * Case 3-1: Execute two dags that have same source and operator chain in separate groups.
   * @throws InjectionException
   */
  @Test
  public void mergingSameSourceAndSameOperatorQueriesSeparateGroupTest()
      throws InjectionException, IOException, ClassNotFoundException {
    // Create a query 1:
    // src1 -> oc1 -> sink1
    final List<String> result1 = new LinkedList<>();
    final String sourceConf = generateConf();
    final String operatorConf = generateConf();
    final TestSource src1 = generateSource(sourceConf);
    final OperatorChain operatorChain1 = generateFilterOperatorChain(operatorConf, (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(generateConf(), result1);
    final DAG<ExecutionVertex, MISTEdge> query1 = generateSimpleQuery(src1, operatorChain1, sink1);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 and operatorChain2 is same as that of src1 and operatorChain2.
    final List<String> result2 = new LinkedList<>();
    final TestSource src2 = generateSource(sourceConf);
    final OperatorChain operatorChain2 = generateFilterOperatorChain(operatorConf, (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(generateConf(), result2);
    final DAG<ExecutionVertex, MISTEdge> query2 = generateSimpleQuery(src2, operatorChain2, sink2);

    // Execute the query 1
    final GroupInfo group1 = generateGroupInfo("g1");
    final GroupInfo group2 = generateGroupInfo("g2");
    queryMerger.start(group1, query1);

    // Execute the query 2
    queryMerger.start(group2, query2);

    final ExecutionDags<String> executionDags1 = group1.getExecutionDags();
    final ExecutionDags<String> executionDags2 = group2.getExecutionDags();
    Assert.assertEquals(1, executionDags1.size());
    Assert.assertEquals(1, executionDags2.size());
    Assert.assertEquals(query1, executionDags1.get(sourceConf));
    Assert.assertEquals(query2, executionDags2.get(sourceConf));

    // Generate events for the merged query and check if the dag is executed correctly
    final String data = "Hello";
    src1.send(data);
    src2.send(data);
    Assert.assertEquals(1, operatorChain1.numberOfEvents());
    Assert.assertEquals(1, operatorChain2.numberOfEvents());
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(true, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data), result1);
    Assert.assertEquals(Arrays.asList(data), result2);
  }

  /**
   * Case 4: Merging two dags that have same source but different operator chain.
   * @throws InjectionException
   */
  @Test
  public void mergingSameSourceButDifferentOperatorQueriesOneGroupTest()
      throws InjectionException, IOException, ClassNotFoundException {
    // Create a query 1:
    // src1 -> oc1 -> sink1
    final List<String> result1 = new LinkedList<>();
    final String sourceConf = generateConf();
    final TestSource src1 = generateSource(sourceConf);
    final OperatorChain operatorChain1 = generateFilterOperatorChain(generateConf(), (s) -> s.startsWith("Hello"));
    final PhysicalSink<String> sink1 = generateSink(generateConf(), result1);
    final DAG<ExecutionVertex, MISTEdge> query1 = generateSimpleQuery(src1, operatorChain1, sink1);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 is same as that of src1,
    // but the configuration of oc2 is different from that of oc1.
    final List<String> result2 = new LinkedList<>();
    final TestSource src2 = generateSource(sourceConf);
    final OperatorChain operatorChain2 = generateFilterOperatorChain(generateConf(), (s) -> s.startsWith("World"));
    final PhysicalSink<String> sink2 = generateSink(generateConf(), result2);
    final DAG<ExecutionVertex, MISTEdge> query2 = generateSimpleQuery(src2, operatorChain2, sink2);

    // Execute the query 1
    final GroupInfo groupInfo = generateGroupInfo(TestParameters.GROUP_ID);
    queryMerger.start(groupInfo, query1);

    // The query 1 and 2 should be merged and the following dag should be created:
    // src1 -> oc1 -> sink1
    //      -> oc2 -> sink2
    final DAG<ExecutionVertex, MISTEdge> expectedDag = new AdjacentListDAG<>();
    GraphUtils.copy(query1, expectedDag);

    expectedDag.addVertex(operatorChain2);
    expectedDag.addVertex(sink2);
    expectedDag.addEdge(src1, operatorChain2, query2.getEdges(src2).get(operatorChain2));
    expectedDag.addEdge(operatorChain2, sink2, query2.getEdges(operatorChain2).get(sink2));

    // Execute the query 2
    queryMerger.start(groupInfo, query2);

    final ExecutionDags<String> executionDags = groupInfo.getExecutionDags();
    final DAG<ExecutionVertex, MISTEdge> mergedDag = executionDags.get(sourceConf);
    Assert.assertEquals(1, executionDags.size());
    Assert.assertEquals(expectedDag, mergedDag);

    // Generate events for the merged query and check if the dag is executed correctly
    final String data1 = "Hello";
    src1.send(data1);
    Assert.assertEquals(1, operatorChain1.numberOfEvents());
    Assert.assertEquals(1, operatorChain2.numberOfEvents());
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(true, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result1);
    Assert.assertEquals(Arrays.asList(), result2);

    final String data2 = "World";
    src1.send(data2);
    Assert.assertEquals(1, operatorChain1.numberOfEvents());
    Assert.assertEquals(1, operatorChain2.numberOfEvents());
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(true, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result1);
    Assert.assertEquals(Arrays.asList(data2), result2);
  }

  /**
   * Test source that sends data to next operator chains.
   */
  final class TestSource implements PhysicalSource {
    private OutputEmitter outputEmitter;
    private final String id;
    private final String conf;

    TestSource(final String id,
               final String conf) {
      this.id = id;
      this.conf = conf;
    }

    @Override
    public void start() {
      // do nothing
    }

    /**
     * Send the data to the next operator chains.
     * @param data data
     * @param <T> data type
     */
    public <T> void send(final T data) {
      outputEmitter.emitData(new MistDataEvent(data));
    }

    @Override
    public void close() throws Exception {
      // do nothing
    }

    @Override
    public Type getType() {
      return Type.SOURCE;
    }

    @Override
    public void setOutputEmitter(final OutputEmitter emitter) {
      outputEmitter = emitter;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getConfiguration() {
      return conf;
    }
  }

  /**
   * Test sink that receives results and stores them to the list.
   * @param <T>
   */
  final class TestSink<T> implements Sink<T> {
    private final List<T> result;

    public TestSink(final List<T> result) {
      this.result = result;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void handle(final T input) {
      result.add(input);
    }
  }
}