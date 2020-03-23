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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.io.IOUtils;
import org.apache.ratis.util.Preconditions;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SeekableByteChannel;

/**
 * Provides a buffering layer in front of a FileChannel for writing.
 *
 * This class is NOT thread-safe.
 */
public class BufferedFileChannel
    implements Flushable, GatheringByteChannel, SeekableByteChannel {

  private final Closeable file;
  private final FileChannel channel;
  private final ByteBuffer writeBuffer;
  private final boolean allowBypass;
  private boolean forced = true;

  @SuppressWarnings("squid:S2095") // 'raf' can only be closed by client later
  public static BufferedFileChannel open(File file, boolean append,
      ByteBuffer buffer, boolean sync, boolean allowBypass) throws IOException {
    final String mode = sync ? "rws" : "rw";
    final RandomAccessFile raf = new RandomAccessFile(file, mode);
    final FileChannel fc = raf.getChannel();
    if (append) {
      fc.position(fc.size());
    } else {
      fc.truncate(0);
    }
    Preconditions.assertSame(fc.size(), fc.position(), "fc.position");
    return new BufferedFileChannel(fc, buffer, allowBypass, raf);
  }

  @VisibleForTesting
  BufferedFileChannel(FileChannel channel, ByteBuffer byteBuffer,
      boolean allowBypass, Closeable file) {
    this.channel = channel;
    this.writeBuffer = byteBuffer;
    this.file = file;
    this.allowBypass = allowBypass;
  }

  @Override
  public void close() throws IOException {
    if (!isOpen()) {
      return;
    }

    try {
      flush();
      channel.truncate(channel.position());
    } finally {
      IOUtils.closeQuietly(channel);
      IOUtils.closeQuietly(file);
    }
  }

  /**
   * Write any data in the buffer to the file and force a
   * sync operation so that data is persisted to the disk.
   *
   * @throws IOException if the write or sync operation fails.
   */
  @Override
  public void flush() throws IOException {
    flushBuffer();
    if (!forced) {
      channel.force(false);
      forced = true;
    }
  }

  @Override
  public int read(ByteBuffer dst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    final int limit = src.limit();
    int offset = 0;
    while (src.hasRemaining()) {
      if (allowBypass && writeBuffer.position() == 0 &&
          src.remaining() >= writeBuffer.limit()) {
        offset += drainBuffer(src);
      } else {
        int toPut = Math.min(src.remaining(), writeBuffer.remaining());
        offset += toPut;
        src.limit(offset);
        try {
          writeBuffer.put(src);
        } finally {
          src.limit(limit);
        }
        if (writeBuffer.remaining() == 0) {
          flushBuffer();
        }
      }
    }
    return offset;
  }

  @Override
  public long position() throws IOException {
    return channel.position();
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    channel.position(newPosition);
    return this;
  }

  @Override
  public long size() throws IOException {
    return channel.size();
  }

  @Override
  public SeekableByteChannel truncate(long size) throws IOException {
    channel.truncate(size);
    return this;
  }

  @Override
  public boolean isOpen() {
    return channel.isOpen();
  }

  @Override
  public long write(ByteBuffer[] srcs, int offset, int length)
      throws IOException {
    long written = 0;
    for (int i = 0; i < length; i++) {
      written += write(srcs[offset + i]);
    }
    return written;
  }

  @Override
  public long write(ByteBuffer[] srcs) throws IOException {
    return write(srcs, 0, srcs.length);
  }

  /**
   * Write any data in the buffer to the file.
   *
   * @throws IOException if the write fails.
   */
  private void flushBuffer() throws IOException {
    if (writeBuffer.position() == 0) {
      return; // nothing to flush
    }

    writeBuffer.flip();
    drainBuffer(writeBuffer);
    writeBuffer.clear();
  }

  /**
   * Write any data in {@code buffer} to the file.
   *
   * @throws IOException if the write fails.
   */
  private int drainBuffer(ByteBuffer buffer) throws IOException {
    int written = 0;
    try {
      do {
        written += channel.write(buffer);
      } while (buffer.hasRemaining());
    } finally {
      if (written > 0) {
        forced = false;
      }
    }
    return written;
  }
}
