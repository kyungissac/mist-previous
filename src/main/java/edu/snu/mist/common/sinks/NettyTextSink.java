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
package edu.snu.mist.common.sinks;

import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.parameters.OperatorId;
import edu.snu.mist.common.parameters.SocketServerIp;
import edu.snu.mist.common.parameters.SocketServerPort;
import edu.snu.mist.common.shared.NettySharedResource;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.io.IOException;

/**
 * This class receives text data stream via Netty.
 */
public final class NettyTextSink implements Sink<String> {

  /**
   * Sink id.
   */
  private final Identifier sinkId;

  /**
   * Output emitter.
   */
  private OutputEmitter outputEmitter;

  /**
   * Netty channel.
   */
  private final Channel channel;

  /**
   * Newline delimeter.
   */
  private final String newline = System.getProperty("line.separator");

  @Inject
  public NettyTextSink(
      @Parameter(OperatorId.class) final String sinkId,
      @Parameter(SocketServerIp.class) final String serverAddress,
      @Parameter(SocketServerPort.class) final int port,
      final NettySharedResource sharedResource,
      final StringIdentifierFactory identifierFactory) throws IOException {
    this.sinkId = identifierFactory.getNewInstance(sinkId);
    final Bootstrap clientBootstrap = sharedResource.getClientBootstrap();
    final ChannelFuture channelFuture = clientBootstrap.connect(serverAddress, port);
    channelFuture.awaitUninterruptibly();
    assert channelFuture.isDone();
    if (!channelFuture.isSuccess()) {
      final StringBuilder sb = new StringBuilder("A connection failed at Sink - ");
      sb.append(channelFuture.cause());
      throw new RuntimeException(sb.toString());
    }
    this.channel = channelFuture.channel();
  }

  @Override
  public Identifier getIdentifier() {
    return sinkId;
  }

  @Override
  public void close() throws Exception {
    if (channel != null) {
      channel.close();
    }
  }

  @Override
  public void handle(final String input) {
    if (input.contains(newline)) {
      channel.writeAndFlush(input);
    } else {
      final StringBuilder sb = new StringBuilder();
      sb.append(input);
      sb.append("\n");
      channel.writeAndFlush(sb.toString());
    }
  }
}
