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
package edu.snu.cms.reef.mist.wordaggregator;

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
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.Identifier;
import org.apache.reef.wake.IdentifierFactory;
import org.apache.reef.wake.remote.impl.StringCodec;

import javax.inject.Inject;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Map;

/**
 * A 'WordAggregator' Task.
 */
public final class WordAggregatorTask implements Task {

  private final Map<String, Integer> counts = new HashMap<String, Integer>();
  private int count = 0;
  private final String receiverName;
  private final IdentifierFactory idFac;
  private final NetworkConnectionService ncs;
  private final Identifier connId;
  private long startTime;

  @NamedParameter
  public static class ReceiverName implements Name<String> {
  }

  private static final Logger LOG = Logger.getLogger(WordAggregatorTask.class.getName());

  private String[] splitter(final String sentence) {
    String[] words = sentence.split(" ");
    return words;
  }

  private void counter(final String word) {
    if(counts.containsKey(word)) {
      counts.put(word, counts.get(word)+1);
    } else {
      counts.put(word, 1);
    }
  }

  private class StringMessageHandler implements EventHandler<Message<String>> {
    @Override
    public void onNext(final Message<String> message) {
      final Iterator<String> iter = message.getData().iterator();
      while(iter.hasNext()) {
        count++;
        String[] msg = iter.next().split(":");
        String sentence = msg[1];
        String[] words = splitter(sentence);
        for(String word : words) {
          counter(word);
        }
        LOG.log(Level.INFO, receiverName + "|Count =" + count);
        for(Map.Entry<String, Integer> item : counts.entrySet()) {
          LOG.log(Level.INFO, receiverName + "|Word: "+ item.getKey() + ", Count: " + item.getValue());
        }
        long currentTime = System.currentTimeMillis();
        System.out.println((currentTime - startTime)+"\t"+(currentTime-Long.parseLong(msg[0])));
      }
    }
  }

  @Inject
  private WordAggregatorTask(final NetworkConnectionService ncs,
                             @Parameter(ReceiverName.class) final String receiverName)
      throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    idFac = injector.getNamedInstance(NetworkConnectionServiceIdFactory.class);
    connId = idFac.getNewInstance("connection");
    final Identifier receiverId = idFac.getNewInstance(receiverName);
    this.ncs = ncs;
    this.ncs.registerConnectionFactory(connId, new StringCodec(), new StringMessageHandler(),
        new WordAggregatorLinkListener(), receiverId);
    this.receiverName = receiverName;
    LOG.log(Level.FINE, "Receiver Task " + this.receiverName + " Started");
  }

  @Override
  public byte[] call(final byte[] memento) {
    startTime = System.currentTimeMillis();
    String senderName = "sender";
    final Identifier receiverId = idFac.getNewInstance(senderName);
    ConnectionFactory<String> connFac = ncs.getConnectionFactory(connId);
    Connection<String> conn = connFac.newConnection(receiverId);
    try {
	    conn.open();
	    conn.write(receiverName);
	    conn.close();
    } catch (NetworkException e) {
	e.printStackTrace(); 
    }

    try {
      Object obj = new Object();
      synchronized (obj) {
          obj.wait(500*1000);
      }
    } catch (InterruptedException e2) {
	e2.printStackTrace(); 
    }
    return null;
  }

}

