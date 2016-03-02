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

package edu.snu.mist.examples;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple sink server to print every received sentence.
 */
public final class SourceServer implements Runnable {
  private final ExecutorService executorService;
  private final ServerSocket serverSocket;
  private final List<PrintWriter> writers;

  SourceServer(final int port) throws IOException {
    writers = new ArrayList<>(10000);
    executorService = Executors.newFixedThreadPool(101);
    serverSocket = new ServerSocket(port);
  }

  @Override
  public void run() {
    try {
      executorService.execute(new Sender());
      while(true) {
        System.out.println("SourceServer running");
        executorService.execute(new Handler(serverSocket.accept()));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  class Sender implements Runnable {
    Sender() {
    }
    public void run() {
      while(true) {
        synchronized (writers) {
          for(PrintWriter writer: writers) {
            writer.println("source\t"+System.currentTimeMillis()+"\t");
          }
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  class Handler implements Runnable {
    private final Socket socket;
    Handler(final Socket socket) {
      this.socket = socket;
    }
    public void run() {
      synchronized (writers) {
        try {
          writers.add(new PrintWriter(socket.getOutputStream(), true));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}

