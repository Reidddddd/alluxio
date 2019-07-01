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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.Sessions;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.BlockWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service in worker side. It is mainly for worker to read blocks from remote workers.
 */
@ThreadSafe
public class BlockTransferService {
  private static final Logger LOG = LoggerFactory.getLogger(BlockTransferService.class);

  /** Block master client */
  private final BlockMasterClient mMasterClient;
  /** Block worker */
  private final BlockWorker mBlockWorker;

  /** A cache to store worker id, and ip information. */
  private final Map<Long, InetSocketAddress> mWorkersAddressCache = new ConcurrentHashMap<>();
  /** Lists of blocks received from master, in following format [id, size, workerid]. */
  private final LinkedList<Long> mRawBlocks = new LinkedList<>();
  /** Used for storing {@link PeerBlockInfo} */
  private final BlockingQueue<PeerBlockInfo> mRemoteBlocks = new LinkedBlockingQueue<>();

  /** Threads pool for running producers and consumers. */
  private final ExecutorService mPools;

  private long mBandwidthLimit = 0L;

  /**
   * Constructor for BlockTransferService.
   * @param blockWorker block worker
   * @param masterClient block master client
   */
  public BlockTransferService(BlockWorker blockWorker, BlockMasterClient masterClient) {
    mBlockWorker = blockWorker;
    mMasterClient = masterClient;
    int nThreads = Configuration.getInt(PropertyKey.WORKER_BLOCK_TRANSFER_THREADS);
    mPools = Executors.newFixedThreadPool(nThreads * 3,
      ThreadFactoryUtils.build("block-transfer-%d", true));
    // To consume faster, make it twice of producer.
    for (int i = 0; i < nThreads; i++) {
      mPools.submit(new Producer());
    }
    for (int i = 0; i < nThreads * 2; i++) {
      mPools.submit(new Consumer());
    }
    mBandwidthLimit = Configuration.getBytes(PropertyKey.WORKER_BALANCER_BANDWIDTH_LIMIT);
    if (mBandwidthLimit != 0) {
      LOG.info("Transfer bandwidth limit is {}.", mBandwidthLimit);
    }
  }

  /**
   * A list of blocks info comes from master command.
   * @param remoteBlocks remote blocks info
   */
  public void transferBlocksToLocal(List<Long> remoteBlocks) {
    synchronized (mRawBlocks) {
      mRawBlocks.addAll(remoteBlocks);
      mRawBlocks.notifyAll();
    }
  }

  /**
   * Producer generates {@link PeerBlockInfo}.
   */
  private class Producer implements Runnable {
    private String threadName;

    @Override
    public void run() {
      threadName = Thread.currentThread().getName();
      while (true) {
        long blockID, blockSize, sourceID;
        synchronized (mRawBlocks) {
          while (mRawBlocks.isEmpty()) {
            try {
              mRawBlocks.wait();
            } catch (InterruptedException e) {
              LOG.warn("Interrupted happened in thread: {}, msg: {}",
                threadName, e.getMessage());
            }
          }
          blockID = mRawBlocks.removeFirst();
          blockSize = mRawBlocks.removeFirst();
          sourceID = mRawBlocks.removeFirst();
        }
        InetSocketAddress address;
        if (!mWorkersAddressCache.containsKey(sourceID)) {
          WorkerNetAddress sourceAddress;
          try {
            sourceAddress = mMasterClient.getWorkerNetAddress(sourceID);
          } catch (IOException e) {
            LOG.error(e.getMessage());
            continue;
          }
          address = new InetSocketAddress(sourceAddress.getHost(), sourceAddress.getDataPort());
          mWorkersAddressCache.put(sourceID, address);
        } else {
          address = mWorkersAddressCache.get(sourceID);
        }
        LOG.info("Pending block {} from {}", blockID, address);
        try {
          mRemoteBlocks.put(new PeerBlockInfo(blockID, blockSize, address, sourceID));
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while pending blocks, {}", e.getMessage());
        }
      }
    }
  }

  /**
   * Consumer consumes {@link PeerBlockInfo}.
   */
  private class Consumer implements Runnable {
    private String threadName;
    private final AtomicLong currentBytes = new AtomicLong(0L);

    @Override
    public void run() {
      threadName = Thread.currentThread().getName();
      while (true) {
        PeerBlockInfo block = EMPTY;
        try {
          while (mBandwidthLimit != 0 && currentBytes.get() >= mBandwidthLimit) {
            TimeUnit.SECONDS.sleep(5);
            LOG.info("Bandwidth excess {}, waiting.", mBandwidthLimit);
          }
          block = mRemoteBlocks.take();
          LOG.info("Reading Balanced block {} from {}", block.ID, block.source.getHostName());
          currentBytes.addAndGet(block.size);
          mBlockWorker.createBlock(Sessions.TRANSFER_BLOCK_SESSION_ID, block.ID, "MEM", block.size);
          try (PeerBlockReader reader = new PeerBlockReader(block.ID, block.size, block.source)) {
            BlockWriter writer = mBlockWorker.getTempBlockWriterRemote(
              Sessions.TRANSFER_BLOCK_SESSION_ID, block.ID);
            BufferUtils.fastCopy(reader.getChannel(), writer.getChannel());
            mBlockWorker.commitBalancedBlock(Sessions.TRANSFER_BLOCK_SESSION_ID, block.ID, block.sourceId);
            LOG.info("Committed Balanced Block {} from source worker {}", block.ID, block.source.getHostName());
          }
        } catch (InterruptedException | IOException | BlockDoesNotExistException |
          BlockAlreadyExistsException | InvalidWorkerStateException |
          WorkerOutOfSpaceException e) {
          LOG.error("Thread: {}, error: {}.", threadName, e.getMessage());
          try {
            mBlockWorker.abortBlock(Sessions.TRANSFER_BLOCK_SESSION_ID, block.ID);
          } catch (BlockAlreadyExistsException | BlockDoesNotExistException |
            InvalidWorkerStateException | IOException ee) {
            LOG.error("Thread: {}, error: {}.", threadName, ee.getMessage());
          }
        } finally {
          if (!block.isEmpty()) {
            currentBytes.addAndGet(-block.size);
          }
        }
      }
    }
  }

  private final PeerBlockInfo EMPTY = new PeerBlockInfo(-1, -1, null, -1);

  /**
   * A POJO stores basic info aboult block's info, id, size, where is it.
   */
  private class PeerBlockInfo {
    long ID;
    long size;
    InetSocketAddress source;
    long sourceId;

    PeerBlockInfo(long blockID, long blockSize, InetSocketAddress source, long sourceId) {
      this.ID = blockID;
      this.size = blockSize;
      this.source = source;
      this.sourceId = sourceId;
    }

    boolean isEmpty() {
      return ID == -1 || size == -1 || source == null || sourceId == -1;
    }
  }
}
