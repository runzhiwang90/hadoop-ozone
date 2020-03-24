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
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG =
      LoggerFactory.getLogger(BufferedFileChannel.class);

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
    Preconditions.checkState(fc.size() == fc.position(), "fc.position");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opened {} for write, position: {}, size: {}", file,
          fc.position(), fc.size());
    }
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
  public int read(ByteBuffer dst) throws IOException {
    int read = 0;
    final long position = channel.position();
    if (dst.hasRemaining() && position < channel.size()) {
      read += channel.read(dst);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read {} bytes from file from position {}", read, position);
      }
    }
    int copied = copy(writeBuffer, dst);
    if (copied > 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Copied {} bytes from buffer", copied);
      }
    }
    read += copied;
    return read;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int written = 0;
    while (src.hasRemaining()) {
      if (allowBypass && writeBuffer.position() == 0 &&
          src.remaining() >= writeBuffer.limit()) {
        written += drainBuffer(src);
      } else {
        int copied = copy(src, writeBuffer);
        written += copied;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Written {} bytes into buffer, {} bytes remaining",
              copied, writeBuffer.remaining());
        }
        if (writeBuffer.remaining() == 0) {
          flushBuffer();
        }
      }
    }
    return written;
  }

  @Override
  public long position() throws IOException {
    long filePosition = channel.position();
    if (filePosition < channel.size()) {
      return filePosition;
    }

    return filePosition + writeBuffer.position();
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    Preconditions.checkArgument(newPosition >= 0);

    long filePosition = channel.position();
    long fileSize = channel.size();
    long bufferPosition = writeBuffer.position();
    long bufferSize = writeBuffer.limit();

    if (LOG.isTraceEnabled()) {
      LOG.trace("Current position {}+{}, size {}+{}, seeking to {}",
          filePosition, bufferPosition, fileSize, bufferSize, newPosition);
    }

    if (newPosition <= fileSize) {
      channel.position(newPosition);
      writeBuffer.position(0);
    } else if (newPosition <= fileSize + bufferSize) {
      channel.position(fileSize);
      writeBuffer.position(Math.toIntExact(newPosition - fileSize));
    } else {
      throw new UnsupportedOperationException(String.format("Seeking beyond " +
          "end of file is not supported: %s + %s < %s",
          fileSize, bufferSize, newPosition));
    }

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
    int written = drainBuffer(writeBuffer);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Written {} bytes to file, new position: {}, new size: {}",
          written, channel.position(), channel.size());
    }
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

  private static int copy(ByteBuffer src, ByteBuffer dst) {
    final int toPut = Math.min(src.remaining(), dst.remaining());
    if (toPut > 0) {
      final int limit = src.limit();
      src.limit(src.position() + toPut);
      try {
        dst.put(src);
      } finally {
        src.limit(limit);
      }
    }
    return toPut;
  }

}
