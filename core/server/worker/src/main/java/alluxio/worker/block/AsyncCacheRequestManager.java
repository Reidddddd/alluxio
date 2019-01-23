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

import alluxio.Constants;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.executor.DynamicShiftQueue;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.*;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Handles client requests to asynchronously cache blocks. Responsible for managing the local
 * worker resources and intelligent pruning of duplicate or meaningless requests.
 */
@ThreadSafe
public class AsyncCacheRequestManager {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncCacheRequestManager.class);

  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();
  /** Executor service for execute the async cache tasks. */
  private final ExecutorService mLocalCacheExecutor;
  private final ExecutorService mRemoteCacheExecutor;
  /** The block worker. */
  private final BlockWorker mBlockWorker;
  private final String mLocalWorkerHostname;

  private final LinkedBlockingQueue<CacheBlock> pendingCache = new LinkedBlockingQueue<>();

  private final DynamicShiftQueue<Runnable> dynamicShiftQueue;

  /**
   * @param service thread pool to run the background caching work
   * @param blockWorker handler to the block worker
   */
  public AsyncCacheRequestManager(ExecutorService service, BlockWorker blockWorker) {
    mBlockWorker = blockWorker;
    mLocalWorkerHostname = NetworkAddressUtils.getLocalHostName();

    int core = Runtime.getRuntime().availableProcessors();
    int capacityA = core / 2;
    int capacityB = core - capacityA;
    dynamicShiftQueue = new DynamicShiftQueue<>(capacityA, capacityB);
    mLocalCacheExecutor = new ThreadPoolExecutor(capacityA, capacityA, Constants.SECOND_MS * 10,
      TimeUnit.MILLISECONDS, dynamicShiftQueue.getDynamicQueueA());
    mRemoteCacheExecutor = new ThreadPoolExecutor(capacityB, capacityB, Constants.SECOND_MS * 10,
      TimeUnit.MILLISECONDS, dynamicShiftQueue.getDynamicQueueB());
    service.submit(new Picker());
  }

  private class Picker implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          CacheBlock block = pendingCache.take();
          if (block.getSource().getHostName().equals(mLocalWorkerHostname)) {
            mLocalCacheExecutor.submit(new LocalCacheTask(block));
          } else {
            mRemoteCacheExecutor.submit(new RemoteCacheTask(block));
          }
        } catch (Exception e) {
          LOG.error("Exception while caching local {}", e.getMessage());
        }
      }
    }
  }

  private class LocalCacheTask implements Runnable {
    CacheBlock block;
    LocalCacheTask(CacheBlock block) {
      this.block = block;
    }
    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      cacheBlockFromUfs(block.getId(), block.getLength(), block.getOptions());
      long endTime = System.currentTimeMillis();
      LOG.info("Time to cache {} is {}ms", block.getId(), endTime - startTime);
      ASYNC_CACHE_UFS_BLOCKS.inc();
    }
  }

  private class RemoteCacheTask implements Runnable {
    CacheBlock block;
    RemoteCacheTask(CacheBlock block) {
      this.block = block;
    }
    @Override
    public void run() {
      long startTime = System.currentTimeMillis();
      cacheBlockFromRemoteWorker(block.getId(), block.getLength(),
        block.getSource(), block.getOptions());
      long endTime = System.currentTimeMillis();
      LOG.info("Time to cache {} is {]ms", block.getId(), endTime - startTime);
      ASYNC_CACHE_REMOTE_BLOCKS.inc();
    }
  }

  static class CacheBlock {
    long id;
    long length;
    Protocol.OpenUfsBlockOptions options;
    InetSocketAddress source;

    private CacheBlock(long id, long length, Protocol.OpenUfsBlockOptions options,
      InetSocketAddress source) {
      this.id = id;
      this.length = length;
      this.options = options;
      this.source = source;
    }

    public long getId() {
      return id;
    }

    public long getLength() {
      return length;
    }

    public Protocol.OpenUfsBlockOptions getOptions() {
      return options;
    }

    public InetSocketAddress getSource() {
      return source;
    }

    static class Builder {
      long bid;
      long blen;
      Protocol.OpenUfsBlockOptions openUfsOptions;
      InetSocketAddress source;

      public Builder setBlock(long bid) {
        this.bid = bid;
        return this;
      }

      public Builder setLength(long blen) {
        this.blen = blen;
        return this;
      }

      public Builder setOpenUfsOptions(Protocol.OpenUfsBlockOptions openUfsOptions) {
        this.openUfsOptions = openUfsOptions;
        return this;
      }

      public Builder setSource(InetSocketAddress source) {
        this.source = source;
        return this;
      }

      public CacheBlock build() {
        return new CacheBlock(bid, blen, openUfsOptions, source);
      }
    }
  }

  /**
   * Handles a request to cache a block asynchronously. This is a non-blocking call.
   *
   * @param request the async cache request fields will be available
   */
  public void submitRequest(Protocol.AsyncCacheRequest request) {
    ASYNC_CACHE_REQUESTS.inc();
    long blockId = request.getBlockId();
    long blockLength = request.getLength();
    long lockID = mBlockWorker.lockBlockNoException(Sessions.ASYNC_CACHE_SESSION_ID, blockId);
    if (lockID != BlockLockManager.INVALID_LOCK_ID) {
      try {
        mBlockWorker.unlockBlock(lockID);
      } catch (BlockDoesNotExistException bdee) {
        LOG.error("Failed to unlock block on async caching. We should never reach here", bdee);
      }
      ASYNC_CACHE_DUPLICATE_REQUESTS.inc();
      return;
    } else {
      CacheBlock.Builder builder = new CacheBlock.Builder();
      builder.setBlock(blockId).setLength(blockLength)
             .setOpenUfsOptions(request.getOpenUfsBlockOptions())
             .setSource(new InetSocketAddress(request.getSourceHost(), request.getSourcePort()));
      CacheBlock cacheBlock = builder.build();
      pendingCache.add(cacheBlock);
    }
  }

  /**
   * Caches the block via the local worker to read from UFS.
   *
   * @param blockId block ID
   * @param blockSize block size
   * @param openUfsBlockOptions options to open the UFS file
   * @return if the block is cached
   */
  private boolean cacheBlockFromUfs(long blockId, long blockSize,
      Protocol.OpenUfsBlockOptions openUfsBlockOptions) {
    // Check if the block has been requested in UFS block store
    try {
      if (!mBlockWorker
          .openUfsBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId, openUfsBlockOptions)) {
        LOG.warn("Failed to async cache block {} from UFS on opening the block", blockId);
        return false;
      }
    } catch (BlockAlreadyExistsException e) {
      // It is already cached
      return true;
    }
    try (BlockReader reader = mBlockWorker
        .readUfsBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId, 0)) {
      // Read the entire block, caching to block store will be handled internally in UFS block store
      // Note that, we read from UFS with a smaller buffer to avoid high pressure on heap
      // memory when concurrent async requests are received and thus trigger GC.
      long offset = 0;
      while (offset < blockSize) {
        long bufferSize = Math.min(8 * Constants.MB, blockSize - offset);
        reader.read(offset, bufferSize);
        offset += bufferSize;
      }
    } catch (AlluxioException | IOException e) {
      // This is only best effort
      LOG.warn("Failed to async cache block {} from UFS on copying the block: {}", blockId, e);
      return false;
    } finally {
      try {
        mBlockWorker.closeUfsBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId);
      } catch (AlluxioException | IOException ee) {
        LOG.warn("Failed to close UFS block {}: {}", blockId, ee);
        return false;
      }
    }
    return true;
  }

  /**
   * Caches the block at best effort from a remote worker (possibly from UFS indirectly).
   *
   * @param blockId block ID
   * @param blockSize block size
   * @param sourceAddress the source to read the block previously by client
   * @param openUfsBlockOptions options to open the UFS file
   * @return if the block is cached
   */
  private boolean cacheBlockFromRemoteWorker(long blockId, long blockSize,
      InetSocketAddress sourceAddress, Protocol.OpenUfsBlockOptions openUfsBlockOptions) {
    try {
      mBlockWorker.createBlockRemote(Sessions.ASYNC_CACHE_SESSION_ID, blockId,
          mStorageTierAssoc.getAlias(0), blockSize);
    } catch (BlockAlreadyExistsException e) {
      // It is already cached
      return true;
    } catch (AlluxioException | IOException e) {
      LOG.warn(
          "Failed to async cache block {} from remote worker ({}) on creating the temp block: {}",
          blockId, sourceAddress, e.getMessage());
      return false;
    }
    try (BlockReader reader =
        new RemoteBlockReader(blockId, blockSize, sourceAddress, openUfsBlockOptions);
        BlockWriter writer =
            mBlockWorker.getTempBlockWriterRemote(Sessions.ASYNC_CACHE_SESSION_ID, blockId)) {
      BufferUtils.fastCopy(reader.getChannel(), writer.getChannel());
      mBlockWorker.commitBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId);
      return true;
    } catch (AlluxioException | IOException e) {
      LOG.warn("Failed to async cache block {} from remote worker ({}) on copying the block: {}",
          blockId, sourceAddress, e.getMessage());
      try {
        mBlockWorker.abortBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId);
      } catch (AlluxioException | IOException ee) {
        LOG.warn("Failed to abort block {}: {}", blockId, ee.getMessage());
      }
      return false;
    }
  }

  // Metrics
  private static final Counter ASYNC_CACHE_REQUESTS = MetricsSystem.counter("AsyncCacheRequests");
  private static final Counter ASYNC_CACHE_DUPLICATE_REQUESTS =
      MetricsSystem.counter("AsyncCacheDuplicateRequests");
  private static final Counter ASYNC_CACHE_FAILED_BLOCKS =
      MetricsSystem.counter("AsyncCacheFailedBlocks");
  private static final Counter ASYNC_CACHE_REMOTE_BLOCKS =
      MetricsSystem.counter("AsyncCacheRemoteBlocks");
  private static final Counter ASYNC_CACHE_SUCCEEDED_BLOCKS =
      MetricsSystem.counter("AsyncCacheSucceededBlocks");
  private static final Counter ASYNC_CACHE_UFS_BLOCKS =
      MetricsSystem.counter("AsyncCacheUfsBlocks");
}
