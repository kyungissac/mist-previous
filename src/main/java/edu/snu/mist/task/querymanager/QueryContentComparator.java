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

import java.util.Comparator;

public final class QueryContentComparator implements Comparator<QueryContent> {
  @Override
  public int compare(final QueryContent c1, final QueryContent c2) {
    if (c1.getLatestActiveTime() - c2.getLatestActiveTime() > 0) {
      return 1;
    } else if (c1.getLatestActiveTime() - c2.getLatestActiveTime() < 0) {
      return -1;
    } else {
      return 0;
    }
  }
}
