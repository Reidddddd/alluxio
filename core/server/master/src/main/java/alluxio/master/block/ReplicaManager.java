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

import alluxio.master.block.meta.MasterBlockInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Replica manager keeps track of those blocks whose number is more than 2.
 * Level means the number of replicas.
 */
@NotThreadSafe
public class ReplicaManager {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaManager.class);

  /** A constant represent 1 replica. */
  private static final int ONLY_ONE_REPLICA = 1;

  /** [level -> block's id that belongs to this level]. */
  private final ConcurrentNavigableMap<Integer, Set<Long>> mReplicaMap;
  /** Invalid blocks which are no longer existed in BlockMap. */
  private final Set<Long> mInvalidReplicas;

  enum ReplicaAction {
    /**
     * PROMOTE means the replica of a block increase, so this block is promoted from lower level to
     * higher level.
     */
    PROMOTE,
    /**
     * EVICT means the replica of a block decrease, so this block is evicted from higher level to
     * lower level.
     */
    EVICT,
    /**
     * TRANSFER means blocks in transfer.
     */
    TRANSFER
  }

  /**
   * Constructor for ReplicaManager.
   */
  public ReplicaManager() {
    mReplicaMap = new ConcurrentSkipListMap<>();
    mInvalidReplicas = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
  }

  /**
   * Promote of evict a block between levels.
   * @param blockId block id
   * @param blockInfo block information
   * @param action PROMOTE or EVICT
   */
  public void replicaPromoteOrEvict(long blockId, MasterBlockInfo blockInfo,
      ReplicaAction action, String worker) {
    int currentLevel = blockInfo.getNumLocations();
    if (currentLevel == ONLY_ONE_REPLICA && action == ReplicaAction.PROMOTE) {
      // No need to track one replica block.
      return;
    }
    createLevelIfNotExisted(currentLevel);

    int previousLevel = action == ReplicaAction.PROMOTE ? currentLevel - 1 : currentLevel + 1;
    if (action == ReplicaAction.TRANSFER) {
      LOG.info("Block {} in transfer. no need to update RM", blockId);
      promoteBlockTo(previousLevel, blockId);
      return;
    }
    int exactLevel = previousLevel;
    if (!contains(previousLevel, blockId) && previousLevel > ONLY_ONE_REPLICA) {
      LOG.warn("Cannot {} block {} in level {}, because it cannot be founded, "
        + "try to find it in all levels.", action, blockId, previousLevel);
      boolean found = false;
      for (int l : getReplicaLevels()) {
        if (contains(l, blockId)) {
          exactLevel = l;
          found = true;
          LOG.warn("Found block {} in level {}. [At host {}]", blockId, exactLevel, worker);
          break;
        }
      }
      if (!found) {
        LOG.warn("Can't find block {} in all levels, put it into level {}. [At host {}]",
          blockId, currentLevel, worker);
        promoteBlockTo(currentLevel, blockId);
        return;
      }
    }

    if (exactLevel == currentLevel) {
      LOG.warn("Block {}'s previous level is equal to current level. [At host {}]",
        blockId, worker);
      return;
    }

    if (exactLevel == previousLevel) {
      LOG.debug("{} block {} from level {} to level {}. [At host {}]",
        action, blockId, exactLevel, currentLevel, worker);
    } else {
      LOG.warn("MOVE block {} from level {} to level {}. [At host {}]",
        blockId, exactLevel, currentLevel, worker);
    }
    evictBlockFrom(exactLevel, blockId);
    promoteBlockTo(currentLevel, blockId);
  }

  /**
   * Get all blocks that equals to or greater than specified level.
   * @param level level of replica
   * @return blocks that >= level
   */
  public Set<Long> fetchBlocksAboveLevel(int level) {
    // Remove invalid blocks before fetching.
    Iterator<Long> invalidIter = mInvalidReplicas.iterator();
    while (invalidIter.hasNext()) {
      long invalidBlock = invalidIter.next();
      for (int l : getReplicaLevels()) {
        if (contains(l, invalidBlock)) {
          evictBlockFrom(l, invalidBlock);
        }
      }
      invalidIter.remove();
    }

    // Fetch all blocks >= level
    Set<Long> blocks = new HashSet<>();
    for (int i : getReplicaLevelGreaterThan(level)) {
      // To avoid ConcurrentModificationException while iteration
      Set<Long> subSet = getBlocksOfLevel(i);
      synchronized (subSet) {
        Iterator<Long> it = subSet.iterator();
        while (it.hasNext()) {
          blocks.add(it.next());
        }
      }
    }
    if (blocks.size() == 0) {
      LOG.info("No replica is more than {}.", level);
    }
    return blocks;
  }

  /**
   * Remove invalid block that no longer in block map.
   * @param blockId id of block
   */
  public void removeInvalidBlock(long blockId) {
    mInvalidReplicas.add(blockId);
  }

  /**
   * Get levels greater than or equals to fromLevel.
   * @param fromLevel specified level
   * @return key set greater than fromLevel
   */
  private Set<Integer> getReplicaLevelGreaterThan(int fromLevel) {
    if (blocksOfLevelIsNull(fromLevel)) {
      return Collections.emptySet();
    }
    return mReplicaMap.tailMap(fromLevel, true).keySet();
  }

  /**
   * Get all current levels.
   * @return all levels
   */
  private Set<Integer> getReplicaLevels() {
    return mReplicaMap.keySet();
  }

  /**
   * Get all blocks in specified level.
   * @param level level of replica
   * @return blocks in specified level
   */
  private Set<Long> getBlocksOfLevel(int level) {
    return mReplicaMap.get(level);
  }

  /**
   * To judge whether given level is existed.
   * @param level level of replica
   * @return true if not existed, false otherwise
   */
  private boolean blocksOfLevelIsNull(int level) {
    return getBlocksOfLevel(level) == null;
  }

  /**
   * Create a level in replica map.
   * @param level level of replica
   */
  private void createLevelIfNotExisted(int level) {
    if (level <= ONLY_ONE_REPLICA) {
      return;
    }
    if (!mReplicaMap.containsKey(level)) {
      mReplicaMap.put(level, Collections.synchronizedSet(new HashSet<Long>()));
      LOG.info("Create level {}.", level);
    }
  }

  /**
   * If a given block is in given level.
   * @param level level of replica
   * @param blockId block id
   * @return true if contains, false otherwise
   */
  private boolean contains(int level, long blockId) {
    return !blocksOfLevelIsNull(level) && getBlocksOfLevel(level).contains(blockId);
  }

  /**
   * Evict a given block from a given level
   * @param level level of replica
   * @param blockId block id
   * @return true if eviction succeeded, false otherwise
   */
  private boolean evictBlockFrom(int level, long blockId) {
    return !blocksOfLevelIsNull(level) && getBlocksOfLevel(level).remove(blockId);
  }

  /**
   * Promote a given block from a given level.
   * @param level level of replica
   * @param blockId block id
   * @return true if promotion succeeded, false otherwise
   */
  private boolean promoteBlockTo(int level, long blockId) {
    return !blocksOfLevelIsNull(level) && getBlocksOfLevel(level).add(blockId);
  }
}
