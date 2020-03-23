/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.utils;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link BufferedFileChannel}.
 */
public class TestBufferedFileChannel {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestBufferedFileChannel.class);

  @Test
  public void testWriteFromNonZeroPosition() throws Exception {
    final int size = 10;
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    final FakeFileChannel spy = new FakeFileChannel();
    final BufferedFileChannel subject = new BufferedFileChannel(spy, buffer,
        false, () -> {});
    final ByteBuffer src = ByteBuffer.wrap("xxx01234".getBytes());
    final int initialPosition = 3;
    src.position(initialPosition);
    final int remaining = src.remaining();

    src.position(initialPosition);
    int written = subject.write(src);

    assertEquals(remaining, written);
  }

  @Test
  public void testFlush() throws Exception {
    for (int bufferSize : new int[] {10, 64, 1024}) {
      testFlush(bufferSize, true);
      testFlush(bufferSize, false);
    }
  }

  private static void testFlush(int size, boolean allowBypass)
      throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(size);
    final FakeFileChannel spy = new FakeFileChannel();
    final BufferedFileChannel subject = new BufferedFileChannel(spy, buffer,
        allowBypass, () -> {});

    // write exactly buffer size, then flush.
    int pos = 0;
    pos = writeExactlyBufferSize(size, spy, subject, pos, pos);
    pos = writeLessThanBufferSize(size, spy, subject, pos, pos);
    pos = writeLessThanBufferSizeTwice(size, spy, subject, pos, pos);
    writeMoreThanBufferSize(size, allowBypass, spy, subject, pos, pos);
  }

  private static int writeExactlyBufferSize(int size,
      FakeFileChannel spy, BufferedFileChannel subject,
      int pos, int force) throws IOException {
    spy.assertValues(pos, force);
    subject.write(ByteBuffer.allocate(size));
    pos += size;
    spy.assertValues(pos,  force);
    subject.flush();
    force = pos;
    spy.assertValues(pos, force);
    return pos;
  }

  private static int writeMoreThanBufferSize(int bufferSize,
      boolean allowBypass, FakeFileChannel spy,
      BufferedFileChannel subject, int pos, int force) throws IOException {
    // write more than buffer size, then flush.
    int n = bufferSize*5/2;
    subject.write(ByteBuffer.allocate(n));
    pos += allowBypass ? n : 2*bufferSize;
    spy.assertValues(pos, force);
    subject.flush();
    pos += allowBypass ? 0 : (n-2*bufferSize);
    force = pos;
    spy.assertValues(pos, force);
    return pos;
  }

  private static int writeLessThanBufferSizeTwice(int bufferSize,
      FakeFileChannel spy, BufferedFileChannel subject,
      int pos, int force) throws IOException {
    // write less than buffer size twice, then flush.
    int n = bufferSize*2/3;
    subject.write(ByteBuffer.allocate(n));
    spy.assertValues(pos, force);
    subject.write(ByteBuffer.allocate(n));
    spy.assertValues(pos + bufferSize, force);
    subject.flush();
    pos += 2*n;
    force = pos;
    spy.assertValues(pos, force);
    return pos;
  }

  private static int writeLessThanBufferSize(int bufferSize,
      FakeFileChannel spy, BufferedFileChannel subject,
      int pos, int force) throws IOException {
    // write less than buffer size, then flush.
    int n = bufferSize/2;
    subject.write(ByteBuffer.allocate(n));
    spy.assertValues(pos, force);
    subject.flush();
    pos += n;
    force = pos;
    spy.assertValues(pos, force);
    return pos;
  }

  private static class FakeFileChannel extends FileChannel {
    private long position = 0;
    private long forcedPosition = 0;

    void assertValues(long expectedPosition, long expectedForcedPosition) {
      assertEquals(expectedPosition, position);
      assertEquals(expectedForcedPosition, forcedPosition);
    }

    @Override
    public int read(ByteBuffer dst) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer dst, long pos) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src, long pos) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int write(ByteBuffer src) {
      final int remaining = src.remaining();
      LOG.info("write {} bytes", remaining);
      position += remaining;
      src.position(src.limit());
      return remaining;
    }

    @Override
    public long position() {
      return position;
    }

    @Override
    public FileChannel position(long newPosition) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long size() {
      return position;
    }

    @Override
    public FileChannel truncate(long newSize) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void force(boolean metaData) {
      LOG.info("force at position {}", position);
      forcedPosition = position;
    }

    @Override
    public long transferTo(long pos, long count, WritableByteChannel target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom(ReadableByteChannel src, long pos, long count) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map(MapMode mode, long pos, long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileLock lock(long pos, long size, boolean shared) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock(long pos, long size, boolean shared) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void implCloseChannel() {
      throw new UnsupportedOperationException();
    }
  }
}
