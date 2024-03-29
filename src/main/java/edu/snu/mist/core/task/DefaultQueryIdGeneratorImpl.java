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

import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.core.parameters.QueryIdPrefix;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation of QueryIdGenerator.
 * It appends the number of submitted queries to prefix.
 */
final class DefaultQueryIdGeneratorImpl implements QueryIdGenerator {

  /**
   * The number of submitted queries.
   */
  private final AtomicLong numSubmittedQueries;

  /**
   * Prefix of query id.
   */
  private final String prefix;

  @Inject
  private DefaultQueryIdGeneratorImpl(@Parameter(QueryIdPrefix.class) final String prefix) {
    this.prefix = prefix;
    this.numSubmittedQueries = new AtomicLong();
  }

  @Override
  public String generate(final LogicalPlan logicalPlan) {
    final StringBuilder sb = new StringBuilder();
    sb.append(prefix);
    sb.append(numSubmittedQueries.getAndIncrement());
    return sb.toString();
  }
}
