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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Simple sink server to print every received sentence.
 */
public final class SinkServer implements Runnable {
  private final ExecutorService executorService;
  private final ServerSocket serverSocket;
  private final List<BufferedReader> readers;

  SinkServer(final int port) throws IOException {
    readers = new ArrayList<>(10000);
    executorService = Executors.newFixedThreadPool(101);
    serverSocket = new ServerSocket(port);
  }

  @Override
  public void run() {
    try {
      executorService.execute(new Sender());
      while(true) {
        System.out.println("SinkServer running");
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
        synchronized (readers) {
          for(BufferedReader reader: readers) {
            try {
              final String line = reader.readLine();
              if (line != null) {
                System.out.println(line + "\tsink\t" + System.currentTimeMillis());
              }
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
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
      synchronized (readers) {
        try {
          readers.add(new BufferedReader(new InputStreamReader(socket.getInputStream())));
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}

