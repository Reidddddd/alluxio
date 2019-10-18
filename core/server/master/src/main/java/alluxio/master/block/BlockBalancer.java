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

package alluxio.master.block;

import alluxio.Constants;
import alluxio.master.block.meta.MasterBlockInfo;
import alluxio.master.block.meta.MasterWorkerInfo;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BlockBalancer is responsible for balancing blocks between workers.
 * The goal of it is to make loaded data among workers near the same.
 */
@ThreadSafe
public class BlockBalancer {
  private static final Logger LOG = LoggerFactory.getLogger(BlockBalancer.class);

  /**
   * A set of worker id that belows the average load of cluster,
   * and supposed to receive blocks from workers that loading is above the average.
   */
  private final Set<Long> mReceivers = Collections.newSetFromMap(new ConcurrentHashMap<>());
  /** A plan contains blocks needed to be transferred from source worker. */
  private final LinkedList<SourcePlan> mPlans = new LinkedList<>();
  /** Block map: block id to block's info. */
  private final Map<Long, MasterBlockInfo> mBlock;

  /** Average load of current round. */
  private long mCurrentRoundAvg = -1L;
  /** Bandwidth limit. */
  private volatile long mBandwidthLimit = 0L;

  /**
   * Constructor for BlockBalancer.
   * @param blocks block map
   */
  public BlockBalancer(Map<Long, MasterBlockInfo> blocks) {
    mBlock = blocks;
  }

  /**
   * Every run after balance status check, we should update the receiving workers lists.
   * For some of them may achieve average status in previous round.
   * @param keySet a set of worker id
   */
  public void setReceiver(Set<Long> keySet) {
    mReceivers.clear();
    mReceivers.addAll(keySet);
  }

  public void printPlanDetail() {
    for (SourcePlan sp : mPlans) {
      LOG.warn("SourcePlan information {}", sp.toString());
    }
  }

  public boolean isAllPlanDispatched() {
    /** denote the balance plans are all dispatched.*/
    synchronized (mPlans) {
      return mPlans.isEmpty();
    }
  }

  /**
   * Get a list of blocks info [blockId, blockSize, workerId]. Worker with this will know where to read
   * blocks.
   * @param worker worker info
   * @return a list of blocks if this worker is supposed to receive blocks, empty list otherwise.
   */
  public List<Long> getBlocksForTransfer(DefaultBlockMaster master, MasterWorkerInfo worker) {
    if (!mReceivers.contains(worker.getId())) {
      return Collections.emptyList();
    }
    mReceivers.remove(worker.getId());
    LinkedList<Long> blocksInTransfer = new LinkedList<>();
    // receivable should be > 0;
    long receivable = mCurrentRoundAvg - worker.getUsedBytes();
    if (mBandwidthLimit > 0) {
      receivable = Math.min(mBandwidthLimit, receivable);
    }
    long received = 0L;
    while (received < receivable) {
      SourcePlan source = EMPTY;
      synchronized (mPlans) {
        if (mPlans.isEmpty()) {
          break;
        }
        source = mPlans.removeFirst();
      }
      if (source.isEmptyPlan()) {
        return blocksInTransfer;
      }
      long sourceID = source.getWorkerID();
      for (Iterator<Map.Entry<Long, Long>> it = source.getBlocksIdSize().entrySet().iterator();
           it.hasNext();) {
        Map.Entry<Long, Long> bit = it.next();
        long blockID = bit.getKey();
        long blockSize = bit.getValue();
        MasterBlockInfo block = mBlock.get(blockID);
        if (block == null) {
          it.remove();
          continue;
        }
        if (block.getBlockLocations().contains(worker)) {
          // if this worker already contains this block,
          // we should avoid all replicas in one worker, just skip.
          continue;
        }
        //change block state to transfering
        MasterWorkerInfo sourceWorker = master.getWorker(sourceID);
        if (sourceWorker.getBlocksToBeRemoved().contains(blockID) || sourceWorker.getBalancedRemoveBlocks().contains(blockID)) {
          it.remove();
          continue;
        }
        received += blockSize;
        // List in format [blockID, blockSize, workerID]
        sourceWorker.updateTransferBlock(blockID, MasterWorkerInfo.Transfer.SENDING);
        blocksInTransfer.addLast(blockID);
        blocksInTransfer.addLast(blockSize);
        blocksInTransfer.addLast(sourceID);
        it.remove();
        worker.updateTransferBlock(blockID, MasterWorkerInfo.Transfer.RECEIVING);
        if (received >= receivable) {
          if (!source.planEmpty()) {
            // delete the remaining blocks (fiinish cluster balanced state need go through multiple transfer plan generate and iterate)
            source.blocksIdSize.clear();
            /** 
            synchronized (mPlans) {
              mPlans.addFirst(source);
            }
            */
          }
          return blocksInTransfer;
        }
      }
    }
    return blocksInTransfer;
  }

  /**
   * Every run after balance status check, we should update the cluster average loads.
   * @param avg cluster average load
   */
  public void setCurrentRoundAvg(long avg) {
    mCurrentRoundAvg = avg;
  }

  /**
   * Create a transfer plan for a source worker.
   * @param sender source  worker
   * @param approximateSize sizes about blocks worker should let out
   */
  public void generateTransferPlan(MasterWorkerInfo sender, long approximateSize) {
    SourcePlan sp = new SourcePlan(sender.getId());
    long sizeOfPlan = 0L;
    long skipNum = 0L;
    // Already in transfer blocks and to be removed blocks should be excluded.
    Set<Long> tmpBlocksCanBeSent = Sets.difference(sender.getBlocks(), sender.getBlocksToBeRemoved());
    Set<Long> blocksCanBeSent = Sets.difference(tmpBlocksCanBeSent, sender.getNeedBalancedBlocks()); 
    for (long bid : blocksCanBeSent) {
      MasterBlockInfo block = mBlock.get(bid);
      if (block == null) {
        continue;
      }
      if (sender.getNeedBalancedBlocks().contains(bid)) {
        // skip the block has been added to balancePlan, don't need transfer again.
        skipNum++;
        continue;
      }
      long size = block.getLength();
      sp.addBlockInfo(bid, size);
      sender.addNeedBalancedBlocks(bid);
      sizeOfPlan += size;
      if (sizeOfPlan >= approximateSize) {
        break;
      }
    }
    LOG.info("Plan to transfer {} blocks with size {}MB from {}, skipNum {}",
      blocksCanBeSent.size(),
      (sizeOfPlan / Constants.MB),
      sender.getWorkerAddress().getHost(),
      skipNum);

    synchronized (mPlans) {
      mPlans.add(sp);
    }
  }

  /**
   * Update the global bandwidth limit of cluster.
   * @param bandwidthLimit bandwidth limit in bytes
   */
  public void setBandwidth(long bandwidthLimit) {
    mBandwidthLimit = bandwidthLimit;
  }

  private final SourcePlan EMPTY = new SourcePlan(-1);

  class SourcePlan {
    /** Source worker id. */
    long workerID;
    /** A map of block id to block size. */
    Map<Long, Long> blocksIdSize;

    SourcePlan(long workerID) {
      this.workerID = workerID;
      this.blocksIdSize = new HashMap<>();
    }

    void addBlockInfo(long blockID, long blockSize) {
      blocksIdSize.put(blockID, blockSize);
    }

    long getWorkerID() {
      return workerID;
    }

    Map<Long, Long> getBlocksIdSize() {
      return blocksIdSize;
    }

    boolean planEmpty() {
      return !isEmptyPlan() && blocksIdSize.isEmpty();
    }

    boolean isEmptyPlan() {
      return workerID == -1;
    }

    @Override
    public String toString() {
      return "SourcePlan{" +
              "workerID=" + workerID +
              ", blocksIdSize=" + blocksIdSize.size() +
              '}';
    }
  }
}
