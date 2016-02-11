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

import edu.snu.mist.task.sources.SourceGenerator;

public final class SourceInput<I> {

  private final String queryId;

  private final SourceGenerator src;

  private final I input;

  public SourceInput(final String queryId,
                     final SourceGenerator src,
                     final I input) {
    this.queryId = queryId;
    this.src = src;
    this.input = input;
  }

  public String getQueryId() {
    return queryId;
  }

  public SourceGenerator getSrc() {
    return src;
  }

  public I getInput() {
    return input;
  }
}
