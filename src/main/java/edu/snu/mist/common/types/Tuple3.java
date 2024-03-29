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
package edu.snu.mist.common.types;

import java.util.Arrays;

/**
 * This class is an implementation of 3-dimensional Tuples.
 */
public final class Tuple3<T1, T2, T3> extends TupleImpl {

  public Tuple3(final T1 value1, final T2 value2, final T3 value3) {
    super(Arrays.asList(value1, value2, value3));
  }
}