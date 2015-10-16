/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.cms.reef.mist.wordcounter;

import org.apache.reef.exception.evaluator.NetworkException;
import org.apache.reef.io.network.Connection;
import org.apache.reef.io.network.ConnectionFactory;
import org.apache.reef.io.network.Message;
import org.apache.reef.io.network.NetworkConnectionService;
import org.apache.reef.io.network.impl.config.NetworkConnectionServiceIdFactory;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.DriverMessage;
import org.apache.reef.task.events.SuspendEvent;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.impl.StringCodec;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A 'WordGenerator' Task.
 */
@Unit
public final class WordGeneratorTask implements Task {

  private final Random rand;

  @NamedParameter
  public static final class SenderName implements Name<String> {
  }

  private final String senderName;
  private final NetworkConnectionService ncs;

  private static final Logger LOG = Logger.getLogger(WordGeneratorTask.class.getName());
  private final List<Connection<String>> connectionList;

  private static class WordGeneratorEventHandler<String> implements EventHandler<Message<String>> {
    @Override
    public void onNext(final Message<String> message) {
    }
  }

  private String generator() {
    final String[] sentences = new String[] {"the cow jumped over the moon", "an apple a day keeps the doctor away",
        "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature" };
    final String sentence = sentences[rand.nextInt(sentences.length)];
    return sentence;
  }

  @Inject
  private WordGeneratorTask(final NetworkConnectionService ncs,
                            @Parameter(SenderName.class) final String senderName) throws InjectionException {
    connectionList = new ArrayList<>();
    this.ncs = ncs;
    this.senderName = senderName;
    rand = new Random();
  }

  public class DriverMsgHandler implements EventHandler<DriverMessage> {
    @Override
    public void onNext(final DriverMessage driverMessage) {
      final byte[] message = driverMessage.get().get();
      String receiverName = new String(message);
      final Injector injector = Tang.Factory.getTang().newInjector();
      final IdentifierFactory idFac;
      try {
        idFac = injector.getNamedInstance(NetworkConnectionServiceIdFactory.class);
        final Identifier connId = idFac.getNewInstance("connection");
        final Identifier senderId = idFac.getNewInstance(senderName);
        final Identifier receiverId = idFac.getNewInstance(receiverName);
        ncs.registerConnectionFactory(connId, new StringCodec(), new WordGeneratorEventHandler<String>(),
            new WordCounterLinkListener(), senderId);

        ConnectionFactory<String> connFac = ncs.getConnectionFactory(connId);
        synchronized (connectionList) {
          connectionList.add(connFac.newConnection(receiverId));
        }
      } catch (InjectionException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public byte[] call(final byte[] memento) {
    try {
      while(true) {
        synchronized (connectionList) {
          for (Connection<String> conn : connectionList) {
            conn.open();
            conn.write(generator());
            conn.close();
          }
        }
        Thread.sleep(1000);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (NetworkException e) {
      e.printStackTrace();
    }
    return null;
  }
}

