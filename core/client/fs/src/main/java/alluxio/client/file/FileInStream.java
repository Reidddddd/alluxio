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
import alluxio.retry.CountingRetry;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;

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

  private final URIStatus mStatus;
  private final InStreamOptions mOptions;
  private final AlluxioBlockStore mBlockStore;
  private final FileSystemContext mContext;

  /* Convenience values derived from mStatus, use these instead of querying mStatus. */
  /** Length of the file in bytes. */
  private final long mLength;
  /** Block size in bytes. */
  private final long mBlockSize;
  /** Passive cache */
  private final boolean mPassiveCache;

  /* Underlying stream and associated bookkeeping. */
  /** Current offset in the file. */
  private long mPosition;
  /** Underlying block stream, null if a position change has invalidated the previous stream. */
  private BlockInStream mBlockInStream;
  /** Writing the data into the local worker. */
  protected BlockOutStream mCurrentCacheStream;
  /** The read buffer in file seek. */
  private byte[] mSeekBuffer;

  /** A map of worker addresses to the most recent epoch time when client fails to read from it. */
  private Map<WorkerNetAddress, Long> mFailedWorkers = new HashMap<>();

  protected FileInStream(URIStatus status, InStreamOptions options, FileSystemContext context) {
    mStatus = status;
    mOptions = options;
    mBlockStore = AlluxioBlockStore.create(context);
    mContext = context;

    mLength = mStatus.getLength();
    mBlockSize = mStatus.getBlockSizeBytes();
    mPassiveCache = Configuration.getBoolean(PropertyKey.USER_FILE_PASSIVE_CACHE_ENABLED);

    mPosition = 0;
    mBlockInStream = null;
    mCurrentCacheStream = null;

    int size = (int) Configuration.getBytes(PropertyKey.USER_FILE_SEEK_BUFFER_SIZE_BYTES);
    mSeekBuffer = new byte[size];
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
            mCurrentCacheStream.write(result);
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
            mCurrentCacheStream.write(b, currentOffset, bytesRead);
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

    long originPos = mPosition;
    try {
      seek(pos);
      return read(b, off, len);
    } finally {
      seek(originPos);
    }
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

    final boolean isInCurrentBlock = pos / mBlockSize == mPosition / mBlockSize;
    if (isInCurrentBlock) {
      updateStream();
      if (mBlockInStream.getSource() == BlockInStream.BlockInStreamSource.LOCAL) {
        mPosition = pos;
        mBlockInStream.seek(mPosition % mBlockSize);
        return;
      } else {
        if (pos > mPosition) {
          finishCurrentBlockToPos(pos);
          if (mPosition == pos) {
            return;
          }
        } else if (mPassiveCache && pos < mPosition) {
          finishCurrentBlockToPos(mLength);
        } else {
          closeBlockInStream(mBlockInStream);
        }
      }
    } else {
      if (mBlockInStream != null && mBlockInStream.remaining() > 0 && mPassiveCache) {
        finishCurrentBlockToPos(mLength);
      } else {
        closeBlockInStream(mBlockInStream);
      }
    }

    mPosition = Math.toIntExact(pos / mBlockSize) * mBlockSize;
    updateStream();
    finishCurrentBlockToPos(pos);
  }

  private void finishCurrentBlockToPos(long pos) throws IOException {
    long blockLength = Math.min(Math.abs(pos - mPosition), mBlockInStream.remaining());
    if (blockLength <= 0) {
      return;
    }
    do {
      int read = read(mSeekBuffer, 0, (int) Math.min(mSeekBuffer.length, blockLength));
      blockLength -= read;
    } while (blockLength > 0);
  }

  /**
   * Initializes the underlying block stream if necessary. This method must be called before
   * reading from mBlockInStream.
   */
  private void updateStream() throws IOException {
    if (mBlockInStream != null && mBlockInStream.remaining() > 0) { // can still read from stream
      if (mCurrentCacheStream != null) {
        if (mBlockInStream.remaining() != mCurrentCacheStream.remaining()) {
          LOG.warn("IN remaining {}, OUT remaining {} in {}", mBlockInStream.remaining(),
            mCurrentCacheStream.remaining(), Thread.currentThread().getName());
        }
      }
      return;
    }

    if (mBlockInStream != null && mBlockInStream.remaining() == 0) { // current stream is done
      closeBlockInStream(mBlockInStream);
      closeOrCancelCacheStream();
    }

    /* Create a new stream to read from mPosition. */
    // Calculate block id.
    long blockId = mStatus.getBlockIds().get(Math.toIntExact(mPosition / mBlockSize));
    // Create stream
    mBlockInStream = mBlockStore.getInStream(blockId, mOptions, mFailedWorkers);

    if (mPassiveCache) {
      if (mBlockInStream.getSource() == BlockInStream.BlockInStreamSource.REMOTE ||
          mBlockInStream.getSource() == BlockInStream.BlockInStreamSource.UFS) {
        WorkerNetAddress local = mContext.getLocalWorker();
        if (local != null) {
          try {
            mCurrentCacheStream =
              mBlockStore.getOutStream(blockId, getBlockSize(mPosition), local, OutStreamOptions.defaults());
          } catch (Exception e) {
            if (e instanceof AlreadyExistsException) {
              LOG.info("The block with ID {} is already stored in the target worker, canceling the cache request.", blockId);
            } else {
              LOG.warn("The block with ID {} could not be cached into Alluxio storage: {}",
                blockId, e.toString());
            }
            closeOrCancelCacheStream();
          }
        }
      }
    }
  }

  private long getBlockSize(long pos) {
    // The size of the last block, 0 if it is equal to the normal block size
    long lastBlockSize = mLength % mBlockSize;
    if (mLength - pos > lastBlockSize) {
      return mBlockSize;
    } else {
      return lastBlockSize;
    }
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
      LOG.info("Block {} does not exist when being cancelled.");
    } catch (AlreadyExistsException e) {
      // This happens if two concurrent readers trying to cache the same block. One successfully
      // committed. The other reader sees this.
      LOG.info("Block {} exists.");
    } catch (IOException e) {
      // This happens when there are any other cache stream close/cancel related errors (e.g.
      // server unreachable due to network partition, server busy due to Alluxio worker is
      // busy, timeout due to congested network etc). But we want to proceed since we want
      // the user to continue reading when one Alluxio worker is havingl trouble.
      LOG.info("Closing or cancelling the cache stream encountered IOException {}, reading from "
        + "the regular stream won't be affected.", e.getMessage());
    }
    mCurrentCacheStream = null;
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
}
