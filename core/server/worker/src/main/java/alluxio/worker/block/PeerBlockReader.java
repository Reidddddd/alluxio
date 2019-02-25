/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.FileSystemContext;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.BlockReader;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * Peer block reader, it is for local worker read blocks from other workers,
 * and cache it on local.
 */
public class PeerBlockReader implements BlockReader {
  /** block id. */
  private final long mBlockID;
  /** block size. */
  private final long mBlockSize;
  /** block's source, worker's ip. */
  private final InetSocketAddress mDataSource;

  private BlockInStream mInputStream;
  private ReadableByteChannel mChannel;
  private volatile boolean mClosed;

  /**
   * Constructor of RemoteBlockReader.
   * @param blockID block id
   * @param blockSize block size
   * @param dataSource ip of worker
   */
  public PeerBlockReader(long blockID, long blockSize, InetSocketAddress dataSource) {
    mBlockID = blockID;
    mBlockSize = blockSize;
    mDataSource = dataSource;
    mClosed = false;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    throw new UnsupportedOperationException("RemoteBlockReader#read is not supported");
  }

  @Override
  public long getLength() {
    return mBlockSize;
  }

  @Override
  public ReadableByteChannel getChannel() {
    if (mChannel == null) {
      WorkerNetAddress address = new WorkerNetAddress().setHost(mDataSource.getHostName())
        .setDataPort(mDataSource.getPort());
      mInputStream = BlockInStream.createPeerBlockInStream(FileSystemContext.get(), mBlockID,
        address, BlockInStream.BlockInStreamSource.REMOTE, mBlockSize);
      mChannel = Channels.newChannel(mInputStream);
    }
    return mChannel;
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    if (mInputStream == null || mInputStream.remaining() <= 0) {
      return -1;
    }
    int bytesToRead = (int) Math.min(buf.writableBytes(), mInputStream.remaining());
    return buf.writeBytes(mInputStream, bytesToRead);
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public void close() throws IOException {
    if (isClosed()) {
      return;
    }
    if (mInputStream != null) {
      mInputStream.close();
      mChannel.close();
    }
    mClosed = true;
  }
}
