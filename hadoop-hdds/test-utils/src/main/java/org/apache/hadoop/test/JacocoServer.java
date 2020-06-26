/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.jacoco.core.data.ExecutionData;
import org.jacoco.core.data.ExecutionDataWriter;
import org.jacoco.core.data.SessionInfo;
import org.jacoco.core.runtime.RemoteControlReader;

/**
 * Simple TPC server to collect all the Jacoco coverage data.
 */
public final class JacocoServer {

  private static final int PORT = 6300;

  private static final String DESTINATION_FILE = "/tmp/jacoco-combined.exec";

  private JacocoServer() {
  }

  @SuppressWarnings("checkstyle:EmptyStatement")
  public static void main(String[] args) throws IOException {
    ExecutionDataWriter destination = new SynchronizedExecutionDataWriter(
        new FileOutputStream(DESTINATION_FILE));
    @SuppressWarnings("java:S2095") // closed in shutdown hook
    ServerSocket serverSocket = new ServerSocket(PORT);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        destination.flush();
        serverSocket.close();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }));

    while (true) {
      final Socket socket = serverSocket.accept();
      new Thread(() -> {
        try {
          RemoteControlReader reader =
              new RemoteControlReader(socket.getInputStream());
          reader.setSessionInfoVisitor(destination);
          reader.setExecutionDataVisitor(destination);
          while (reader.read()) {
            ;//read until the end of the stream.
          }
          destination.flush();
        } catch (Exception ex) {
          ex.printStackTrace();
        } finally {
          try {
            socket.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      }).start();
    }

  }

  @SuppressWarnings("java:S1185") // false warning about useless override
  private static class SynchronizedExecutionDataWriter
      extends ExecutionDataWriter {

    SynchronizedExecutionDataWriter(OutputStream output) throws IOException {
      super(output);
    }

    @Override
    public synchronized void flush() throws IOException {
      super.flush();
    }

    @Override
    public synchronized void visitClassExecution(ExecutionData data) {
      super.visitClassExecution(data);
    }

    @Override
    public synchronized void visitSessionInfo(SessionInfo info) {
      super.visitSessionInfo(info);
    }
  }

}
