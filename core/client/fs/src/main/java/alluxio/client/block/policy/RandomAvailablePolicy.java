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

package alluxio.client.block.policy;

import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.policy.options.GetWorkerOptions;
import alluxio.wire.WorkerNetAddress;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Random;

public final class RandomAvailablePolicy implements BlockLocationPolicy {
  private final Random mSeed;

  public RandomAvailablePolicy() {
    mSeed = new Random();
  }

  @Override
  public WorkerNetAddress getWorker(GetWorkerOptions options) {
    List<BlockWorkerInfo> workersInfo = Lists.newArrayList(options.getBlockWorkerInfos());
    int pick = mSeed.nextInt(workersInfo.size());
    BlockWorkerInfo target = workersInfo.get(pick);
    while (target.getCapacityBytes() - target.getUsedBytes() < options.getBlockSize()) {
      pick = mSeed.nextInt(workersInfo.size());
      target = workersInfo.get(pick);
    }
    return target.getNetAddress();
  }
}
