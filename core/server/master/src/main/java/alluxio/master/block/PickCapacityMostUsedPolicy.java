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

import alluxio.master.block.meta.MasterWorkerInfo;

import java.util.Comparator;
import java.util.List;

/**
 * Choose the capacity most used worker to whom an excess replica belongs.
 */
public class PickCapacityMostUsedPolicy implements ReplicaChoicePolicy {

  public PickCapacityMostUsedPolicy() {}

  @Override
  public MasterWorkerInfo pickUpWorker(List<MasterWorkerInfo> workers) {
    workers.sort(Comparator.comparing(MasterWorkerInfo::getAvailableBytes));
    return workers.get(0);
  }
}