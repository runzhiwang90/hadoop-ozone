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
package org.apache.hadoop.ozone.genesis;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.ozone.container.common.utils.BufferedFileChannel;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

/**
 * Benchmark for implementations of writing to files.
 */
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkFileWrite {

  private static final String DEFAULT_TEST_DATA_DIR =
      "target" + File.separator + "test" + File.separator + "data";

  private static final int WRITE_COUNT = 10000;

  /**
   * State for the benchmark.
   */
  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param({"10", "100", "1000", "10000", "100000", "1000000"})
    private int writeSize;

    private File dir;
    private byte[] data;

    private static File getTestDir() throws IOException {
      File dir = new File(DEFAULT_TEST_DATA_DIR).getAbsoluteFile();
      Files.createDirectories(dir.toPath());
      return dir;
    }

    @Setup(Level.Iteration)
    public void setup() throws IOException {
      dir = getTestDir();
      data = randomAlphanumeric(writeSize).getBytes(UTF_8);
    }

    @TearDown(Level.Iteration)
    public void cleanup() {
      FileUtils.deleteQuietly(dir);
    }

    File getDir() {
      return dir;
    }

    byte[] getData() {
      return data;
    }
  }

  /**
   * State for RandomAccessFile-based benchmark.
   */
  @State(Scope.Benchmark)
  public static class RandomAccessFileState extends BenchmarkState {

    @Param({"false"})
    private boolean sync;

    boolean isSync() {
      return sync;
    }
  }

  /**
   * State for BufferedFileChannel-based benchmark.
   */
  @State(Scope.Benchmark)
  public static class BufferedFileChannelState extends RandomAccessFileState {

    @Param({"8000", "16000", "32000", "64000"})
    private int bufferSize;

    @Param({"true", "false"})
    private boolean allowBypass;

    int getBufferSize() {
      return bufferSize;
    }

    boolean isAllowBypass() {
      return allowBypass;
    }
  }

  @Benchmark
  public void bufferedOutputStream(BenchmarkState state, Blackhole sink)
      throws IOException {
    final File file = new File(state.getDir(), randomAlphanumeric(30));
    try (OutputStream fileOut = new FileOutputStream(file);
         OutputStream out = new BufferedOutputStream(fileOut)) {
      benchmarkStream(out, state, sink);
    }
  }

  @Benchmark
  public void randomAccessFile(RandomAccessFileState state, Blackhole sink)
      throws IOException {
    final File file = new File(state.getDir(), randomAlphanumeric(30));
    final String mode = state.isSync() ? "rws" : "rw";
    try (RandomAccessFile raf = new RandomAccessFile(file, mode);
         FileChannel channel = raf.getChannel()) {
      benchmarkChannel(channel, state, sink);
    }
  }

  @Benchmark
  public void bufferedChannel(BufferedFileChannelState state,
      Blackhole sink) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(state.getBufferSize());
    final File file = new File(state.getDir(), randomAlphanumeric(30));
    try (WritableByteChannel channel = BufferedFileChannel.open(file,
        false, buffer, state.isSync(), state.isAllowBypass())) {
      benchmarkChannel(channel, state, sink);
    }
  }

  private void benchmarkStream(OutputStream out,
      BenchmarkState state, Blackhole sink) throws IOException {
    for (int i = 0; i < WRITE_COUNT; i++) {
      out.write(state.getData());
      sink.consume(state.getData());
    }
  }

  private void benchmarkChannel(WritableByteChannel channel,
      BenchmarkState state, Blackhole sink) throws IOException {
    for (int i = 0; i < WRITE_COUNT; i++) {
      final ByteBuffer buffer = ByteBuffer.wrap(state.getData());
      channel.write(buffer);
      sink.consume(buffer);
    }
  }

}
