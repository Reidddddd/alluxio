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

package alluxio.client.file;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.Seekable;
import alluxio.annotation.PublicApi;
import alluxio.client.BoundedStream;
import alluxio.client.PositionedReadable;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.block.stream.BlockOutStream;
import alluxio.client.file.options.InStreamOptions;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.network.netty.NettyRPC;
import alluxio.network.netty.NettyRPCContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.CountingRetry;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A streaming API to read a file. This API represents a file as a stream of bytes and provides a
 * collection of {@link #read} methods to access this stream of bytes. In addition, one can seek
 * into a given offset of the stream to read.
 *
 * This class wraps the block in stream for each of the blocks in the file and abstracts the
 * switching between streams. The backing streams can read from Alluxio space in the local machine,
 * remote machines, or the under storage system.
 *
 * The internal bookkeeping works as follows:
 *
 * 1. {@link #updateStream()} is a potentially expensive operation and is responsible for
 * creating new BlockInStreams and updating {@link #mBlockInStream}. After calling this method,
 * {@link #mBlockInStream} is ready to serve reads from the current {@link #mPosition}.
 * 2. {@link #mPosition} can become out of sync with {@link #mBlockInStream} when seek or skip is
 * called. When this happens, {@link #mBlockInStream} is set to null and no effort is made to
 * sync between the two until {@link #updateStream()} is called.
 * 3. {@link #updateStream()} is only called when followed by a read request. Thus, if a
 * {@link #mBlockInStream} is created, it is guaranteed we read at least one byte from it.
 */
@PublicApi
@NotThreadSafe
public class FileInStream extends InputStream implements BoundedStream, PositionedReadable,
    Seekable {
  private static final Logger LOG = LoggerFactory.getLogger(FileInStream.class);
  private static final int MAX_WORKERS_TO_RETRY =
      Configuration.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_READ_RETRY);
  private static final boolean PASSIVE_CACHE =
    Configuration.getBoolean(PropertyKey.USER_FILE_PASSIVE_CACHE_ENABLED);
  private static final long CHANNEL_TIMEOUT =
    Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private final URIStatus mStatus;
  private final InStreamOptions mOptions;
  /** The outstream options. */
  private OutStreamOptions mOutStreamOptions;
  private final AlluxioBlockStore mBlockStore;
  private final FileSystemContext mContext;

  /* Convenience values derived from mStatus, use these instead of querying mStatus. */
  /** Length of the file in bytes. */
  private final long mLength;
  /** Block size in bytes. */
  private final long mBlockSize;

  /* Underlying stream and associated bookkeeping. */
  /** Current offset in the file. */
  private long mPosition;
  /** Underlying block stream, null if a position change has invalidated the previous stream. */
  private BlockInStream mBlockInStream;
  /**
   * Current block out stream writing the data into the local worker. This is only used when the in
   * stream reads from a remote worker.
   */
  protected BlockOutStream mCurrentCacheStream;

  /** A map of worker addresses to the most recent epoch time when client fails to read from it. */
  private Map<WorkerNetAddress, Long> mFailedWorkers = new HashMap<>();

  private static final Set<Long> alreadCachedBlocks =
    Collections.newSetFromMap(new ConcurrentHashMap<>());

  private static final ExecutorService service = Executors.newFixedThreadPool(8);

  protected FileInStream(URIStatus status, InStreamOptions options, FileSystemContext context) {
    mStatus = status;
    mOptions = options;
    mBlockStore = AlluxioBlockStore.create(context);
    mContext = context;

    mLength = mStatus.getLength();
    mBlockSize = mStatus.getBlockSizeBytes();

    mPosition = 0;
    mBlockInStream = null;

    mOutStreamOptions = OutStreamOptions.defaults();
  }

  /* Input Stream methods */
  @Override
  public int read() throws IOException {
    if (mPosition == mLength) { // at end of file
      return -1;
    }
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;
    while (retry.attempt()) {
      try {
        updateStream();
        int result = mBlockInStream.read();
        if (result != -1) {
          mPosition++;
          if (mCurrentCacheStream != null) {
            try {
              mCurrentCacheStream.write(result);
            } catch (Exception e) {
              if (e instanceof IOException) {
                handleCacheStreamException((IOException) e);
              } else {
                LOG.warn("Closing cache stream due to {}.", e.getMessage());
                closeOrCancelCacheStream();
              }
            }
          }
        }
        return result;
      } catch (UnavailableException | DeadlineExceededException | ConnectException e) {
        lastException = e;
        if (mBlockInStream != null) {
          handleRetryableException(mBlockInStream, e);
          mBlockInStream = null;
        }
      }
    }
    throw lastException;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    }
    if (mPosition == mLength) { // at end of file
      return -1;
    }

    int bytesLeft = len;
    int currentOffset = off;
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;
    while (bytesLeft > 0 && mPosition != mLength && retry.attempt()) {
      try {
        updateStream();
        int bytesRead = mBlockInStream.read(b, currentOffset, bytesLeft);
        if (bytesRead > 0) {
          if (mCurrentCacheStream != null) {
            try {
              mCurrentCacheStream.write(b, currentOffset, bytesLeft);
            } catch (Exception e) {
              if (e instanceof IOException) {
                handleCacheStreamException((IOException) e);
              } else {
                LOG.warn("Closing cache stream due to {}.", e.getMessage());
                closeOrCancelCacheStream();
              }
            }
          }
          bytesLeft -= bytesRead;
          currentOffset += bytesRead;
          mPosition += bytesRead;
        }
        retry.reset();
        lastException = null;
      } catch (UnavailableException | ConnectException | DeadlineExceededException e) {
        lastException = e;
        if (mBlockInStream != null) {
          handleRetryableException(mBlockInStream, e);
          mBlockInStream = null;
        }
      }
    }
    if (lastException != null) {
      throw lastException;
    }
    return len - bytesLeft;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(n, mLength - mPosition);
    seek(mPosition + toSkip);
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    closeBlockInStream(mBlockInStream);
    closeOrCancelCacheStream();
  }

  /* Bounded Stream methods */
  @Override
  public long remaining() {
    return mLength - mPosition;
  }

  /* Positioned Readable methods */
  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    return positionedReadInternal(pos, b, off, len);
  }

  private int positionedReadInternal(long pos, byte[] b, int off, int len) throws IOException {
    if (pos < 0 || pos >= mLength) {
      return -1;
    }

    int lenCopy = len;
    CountingRetry retry = new CountingRetry(MAX_WORKERS_TO_RETRY);
    IOException lastException = null;
    while (len > 0 && retry.attempt()) {
      if (pos >= mLength) {
        break;
      }
      long blockId = mStatus.getBlockIds().get(Math.toIntExact(pos / mBlockSize));
      BlockInStream stream = null;
      try {
        stream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
        long offset = pos % mBlockSize;
        int bytesRead =
            stream.positionedRead(offset, b, off, (int) Math.min(mBlockSize - offset, len));
        Preconditions.checkState(bytesRead > 0, "No data is read before EOF");
        pos += bytesRead;
        off += bytesRead;
        len -= bytesRead;
        retry.reset();
        lastException = null;
      } catch (UnavailableException | DeadlineExceededException | ConnectException e) {
        lastException = e;
        if (stream != null) {
          handleRetryableException(stream, e);
          stream = null;
        }
      } finally {
        closeBlockInStream(stream);
      }
    }
    if (lastException != null) {
      throw lastException;
    }
    return lenCopy - len;
  }

  /* Seekable methods */
  @Override
  public long getPos() {
    return mPosition;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (mPosition == pos) {
      return;
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_FILE.toString(), pos);

    if (mBlockInStream == null) { // no current stream open, advance position
      mPosition = pos;
      return;
    }

    long delta = pos - mPosition;
    if (delta <= mBlockInStream.remaining() && delta >= -mBlockInStream.getPos()) { // within block
      mBlockInStream.seek(mBlockInStream.getPos() + delta);
    } else { // close the underlying stream as the new position is no longer in bounds
      closeBlockInStream(mBlockInStream);
    }
    mPosition += delta;
  }

  /**
   * Initializes the underlying block stream if necessary. This method must be called before
   * reading from mBlockInStream.
   */
  private void updateStream() throws IOException {
    if (mBlockInStream != null && mBlockInStream.remaining() > 0) { // can still read from stream
      if (mCurrentCacheStream != null &&
          mCurrentCacheStream.remaining() != mBlockInStream.remaining()) {
        LOG.info("BlockInStream and CacheStream is out of sync %d %d.",
          mBlockInStream.remaining(), mCurrentCacheStream.remaining());
        closeOrCancelCacheStream();
      }
      return;
    }

    if (mBlockInStream != null && mBlockInStream.remaining() == 0) { // current stream is done
      closeBlockInStream(mBlockInStream);
    }

    /* Create a new stream to read from mPosition. */
    // Calculate block id.
    final long blockId = mStatus.getBlockIds().get(Math.toIntExact(mPosition / mBlockSize));
    // Create stream
    long startTime = System.currentTimeMillis();
    mBlockInStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);
    long endTime = System.currentTimeMillis();
    LOG.info("Time get BlockInStream {}ms", endTime - startTime);

    // Send an async cache request to a worker based on read type and passive cache options.
    boolean cache = mOptions.getOptions().getReadType().isCache();
    if (cache) {
      LOG.info("Cache local!");
      updateCacheStream(blockId);
    }
    /*
    if (cache && !alreadCachedBlocks.contains(blockId)) {
      alreadCachedBlocks.add(blockId);
      LOG.info("Passively caching block {}", blockId);
      service.submit(new Runnable() {
        @Override
        public void run() {
          try {
            passiveCacheRemote(blockId);
          } catch (IOException e) {
            LOG.info("Unexpected!!!!");
          }
        }
      });
    }
    */

    // Set the stream to the correct position.
    long offset = mPosition % mBlockSize;
    LOG.info("Reading block {} at offset {} from {} worker {}.",
      blockId, offset, mBlockInStream.getSource(), mBlockInStream.getAddress().getHost());
    mBlockInStream.seek(offset);
  }

  private void updateCacheStream(long blockId) {
    if (mCurrentCacheStream == null || mCurrentCacheStream.remaining() == 0) {
      closeOrCancelCacheStream();
    }

    if (blockId < 0) {
      // End of file.
      return;
    }

    LOG.info("Source: {}", mBlockInStream.getSource());
    if (mBlockInStream.getSource() == BlockInStream.BlockInStreamSource.LOCAL) {
      return;
    }
    if (getPos() % mBlockSize != 0) {
      return;
    }

    LOG.info("Create local cache stream.");
    try {
      WorkerNetAddress localWorker = mContext.getLocalWorker();
      if (localWorker != null) {
        mCurrentCacheStream =
          mBlockStore.getOutStream(blockId, getBlockSize(getPos()), localWorker, mOutStreamOptions);
      }
    } catch (Exception e) {
      if (e instanceof IOException) {
        handleCacheStreamException((IOException) e);
      } else {
        LOG.warn("Closing cache stream due to {}.", e.getMessage());
        closeOrCancelCacheStream();
      }
    }
  }

  /**
   * If we are not in the last block or if the last block is equal to the normal block size, return
   * the normal block size. Otherwise return the block size of the last block.
   *
   * @param pos the position to get the block size for
   * @return the size of the block that covers pos
   */
  private long getBlockSize(long pos) {
    // The size of the last block, 0 if it is equal to the normal block size
    long lastBlockSize = mStatus.getLength() % mBlockSize;
    if (mStatus.getLength() - pos > lastBlockSize) {
      return mBlockSize;
    } else {
      return lastBlockSize;
    }
  }

  private void passiveCacheRemote(long blockId) throws IOException {
    WorkerNetAddress dataSource = mBlockInStream.getAddress();
    WorkerNetAddress worker;
    if (PASSIVE_CACHE && mContext.hasLocalWorker()) { // send request to local worker
      worker = mContext.getLocalWorker();
    } else { // send request to data source
      worker = mBlockInStream.getAddress();
    }
    try {
      // Construct the async cache request
      long blockLength = mOptions.getBlockInfo(blockId).getLength();
      Protocol.AsyncCacheRequest request =
        Protocol.AsyncCacheRequest.newBuilder().setBlockId(blockId).setLength(blockLength)
          .setOpenUfsBlockOptions(mOptions.getOpenUfsBlockOptions(blockId))
          .setSourceHost(dataSource.getHost()).setSourcePort(dataSource.getDataPort()).build();
      Channel channel = mContext.acquireNettyChannel(worker);
      try {
        NettyRPCContext rpcContext =
          NettyRPCContext.defaults().setChannel(channel).setTimeout(CHANNEL_TIMEOUT);
        NettyRPC.fireAndForget(rpcContext, new ProtoMessage(request));
      } finally {
        mContext.releaseNettyChannel(worker, channel);
      }
    } catch (Exception e) {
      LOG.warn("Failed to complete async cache request for block {} at worker {}: {}", blockId,
        worker, e.getMessage());
    }
  }

  private void closeBlockInStream(BlockInStream stream) throws IOException {
    if (stream != null) {
      // Get relevant information from the stream.
      stream.close();
      // TODO(calvin): we should be able to do a close check instead of using null
      if (stream == mBlockInStream) { // if stream is instance variable, set to null
        mBlockInStream = null;
      }
    }
  }

  private void handleRetryableException(BlockInStream stream, IOException e) {
    WorkerNetAddress workerAddress = stream.getAddress();
    LOG.warn("Failed to read block {} from worker {}, will retry: {}",
        stream.getId(), workerAddress, e.getMessage());
    try {
      stream.close();
    } catch (Exception ex) {
      // Do not throw doing a best effort close
      LOG.warn("Failed to close input stream for block {}: {}", stream.getId(), ex.getMessage());
    }

    mFailedWorkers.put(workerAddress, System.currentTimeMillis());
  }

  /**
   * Handles IO exceptions thrown in response to the worker cache request. Cache stream is closed or
   * cancelled after logging some messages about the exceptions.
   *
   * @param e the exception to handle
   */
  private void handleCacheStreamException(IOException e) {
    if (Throwables.getRootCause(e) instanceof AlreadyExistsException) {
      // This can happen if there are two readers trying to cache the same block. The first one
      // created the block (either as temp block or committed block). The second sees this
      // exception.
      LOG.info("The block is already stored in the target worker, canceling the cache request.");
    } else {
      LOG.warn("The block could not be cached into Alluxio storage: {}", e.toString());
    }
    closeOrCancelCacheStream();
  }

  /**
   * Closes or cancels {@link #mCurrentCacheStream}.
   */
  private void closeOrCancelCacheStream() {
    if (mCurrentCacheStream == null) {
      return;
    }
    try {
      if (mCurrentCacheStream.remaining() == 0) {
        mCurrentCacheStream.close();
      } else {
        mCurrentCacheStream.cancel();
      }
    } catch (NotFoundException e) {
      // This happens if two concurrent readers read trying to cache the same block. One cancelled
      // before the other. Then the other reader will see this exception since we only keep
      // one block per blockId in block worker.
      LOG.info("Block does not exist when being cancelled.");
    } catch (AlreadyExistsException e) {
      // This happens if two concurrent readers trying to cache the same block. One successfully
      // committed. The other reader sees this.
      LOG.info("Block exists.");
    } catch (IOException e) {
      // This happens when there are any other cache stream close/cancel related errors (e.g.
      // server unreachable due to network partition, server busy due to Alluxio worker is
      // busy, timeout due to congested network etc). But we want to proceed since we want
      // the user to continue reading when one Alluxio worker is having trouble.
      LOG.info("Closing or cancelling the cache stream encountered IOException {}, reading from "
        + "the regular stream won't be affected.", e.getMessage());
    }
    mCurrentCacheStream = null;
  }
}
