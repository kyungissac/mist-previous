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
import junit.framework.Assert;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import javax.management.Notification;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

public final class MemoryCheckerTest {
  static final Logger LOG = Logger.getLogger(MemoryCheckerTest.class.getName());

  //@Test(timeout = 4000L)
  public void memoryCheckerNoNotificationTest() throws InjectionException {
    final int memThreshold = 20;
    final long gracePeriod = 1000;
    final Queue<Notification> notifications = new ConcurrentLinkedQueue<>();
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    // Set 20MB threshold
    jcb.bindNamedParameter(MaxHeapMemoryThreshold.class, memThreshold+"");
    // Set 2sec grace period
    jcb.bindNamedParameter(GracePeriod.class, gracePeriod+"");

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(MemoryListener.class, (notification, handback) -> {
      notifications.add(notification);
      LOG.info(notification.toString());
    });
    final MemoryChecker memoryChecker = injector.getInstance(MemoryChecker.class);
    try {
      Thread.sleep(3000);
    } catch (final InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(0, notifications.size());
  }

  @Test(timeout = 22000L)
  public void memoryCheckerNotificationTest() throws InjectionException {
    final int memThreshold = 10;
    final long gracePeriod = 1000;
    final Queue<Notification> notifications = new ConcurrentLinkedQueue<>();
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    // Set 20MB threshold
    jcb.bindNamedParameter(MaxHeapMemoryThreshold.class, memThreshold+"");
    // Set 2sec grace period
    jcb.bindNamedParameter(GracePeriod.class, gracePeriod+"");

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(MemoryListener.class, (notification, handback) -> {
      notifications.add(notification);
      LOG.info(notification.toString());
    });
    final MemoryChecker memoryChecker = injector.getInstance(MemoryChecker.class);
    final List<Integer> list = new LinkedList<>();
    while (notifications.size() < 30000000) {
      list.add(1);
    }
    LOG.info("Size " + list.size());
    Assert.assertTrue("Notification size: " + notifications.size(),
        notifications.size() > 0 && notifications.size() < 4);
  }
}
