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

import edu.snu.mist.task.parameters.GracePeriod;
import edu.snu.mist.task.parameters.MaxHeapMemoryThreshold;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import javax.management.NotificationEmitter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public final class MemoryChecker {

  private final AtomicLong prevHandleTime;

  @Inject
  private MemoryChecker(final MemoryListener memoryListener,
                        @Parameter(MaxHeapMemoryThreshold.class) final int maxHeapMemoryThreshold,
                        @Parameter(GracePeriod.class) final long gracePeriod) {
    this.prevHandleTime = new AtomicLong(System.currentTimeMillis());
    //Start to monitor memory usage
    final MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
    final NotificationEmitter emitter = (NotificationEmitter) mbean;
    emitter.addNotificationListener(memoryListener, notification -> {
      final List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
      for (final MemoryPoolMXBean pool : pools) {

        if(pool.isCollectionUsageThresholdSupported()){
          System.out.println(pool.getName() + ", " + pool.getType() + ", " + pool.getCollectionUsage());
        }
      }
      System.out.println("noti: " + notification.getMessage() +
          ", noti time: " + notification.getTimeStamp() + ", prevtime: " + prevHandleTime.get()
      + ", noti type" + notification.getType());
      String notifType = notification.getType();
      if (notifType.equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {
        System.out.println("yyy");
        if (notification.getTimeStamp() - prevHandleTime.get() > gracePeriod) {
          return prevHandleTime.compareAndSet(
              prevHandleTime.get(), notification.getTimeStamp());
        }
      }
      return false;
    }, null);

    //set threshold
    final List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
    for (final MemoryPoolMXBean pool : pools) {
      System.out.println(pool.getName() + ", " + pool.getType() + ", " + pool.isCollectionUsageThresholdSupported()
          +", " + pool.isUsageThresholdSupported());
      if(pool.isCollectionUsageThresholdSupported()){
        pool.setCollectionUsageThreshold(maxHeapMemoryThreshold * 1000000);
      }
    }
  }
}