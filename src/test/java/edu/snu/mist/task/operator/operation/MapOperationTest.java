/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.mist.task.operator.operation;

import com.google.common.collect.ImmutableList;
import edu.snu.mist.task.operator.operation.immediate.MapOperation;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.IdentifierFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public final class MapOperationTest {

  /**
   * Test map operation.
   * It converts string to tuple (string, 1).
   */
  @Test
  public void testMapOperation() throws InjectionException {
    // input stream
    final ImmutableList<String> inputStream = ImmutableList.of("a", "b", "d", "b", "c");
    // expected output
    final Tuple[] outputs = {new Tuple("a", 1), new Tuple("b", 1),
        new Tuple("d", 1), new Tuple("b", 1), new Tuple("c", 1)};
    final List<Tuple> expected = Arrays.asList(outputs);
    // actual result
    final List<Tuple> result = new ArrayList<>();

    final Injector injector = Tang.Factory.getTang().newInjector();
    // map function: convert string to tuple
    final Function<String, Tuple> mapFunc = (mapInput) -> new Tuple(mapInput, 1);

    final IdentifierFactory idfac = injector.getInstance(StringIdentifierFactory.class);

    injector.bindVolatileInstance(Function.class, mapFunc);
    final MapOperation<String, Tuple> mapOperation = injector.getInstance(MapOperation.class);
    // execute map operation
    final ImmutableList<Tuple> actualOutputs = mapOperation.compute(inputStream);
    result.addAll(actualOutputs);

    System.out.println("expected: " + expected);
    System.out.println("result: " + result);
    Assert.assertEquals(expected, result);
  }

  class Tuple {
    private final String key;
    private final int val;

    Tuple(final String key, final int val) {
      this.key = key;
      this.val = val;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final Tuple tuple = (Tuple) o;

      if (val != tuple.val) {
        return false;
      }
      if (!key.equals(tuple.key)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      int result = key.hashCode();
      result = 31 * result + val;
      return result;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      sb.append("(");
      sb.append(this.key);
      sb.append(",");
      sb.append(this.val);
      sb.append(")");
      return sb.toString();
    }
  }
}