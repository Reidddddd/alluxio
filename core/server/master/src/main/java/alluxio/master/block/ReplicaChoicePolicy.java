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
import alluxio.util.CommonUtils;

import java.util.List;

/**
 * Policy to choose a worker to whom an excess replica belongs.
 */
interface ReplicaChoicePolicy {

  class Factory {
    private Factory() {}

    public static ReplicaChoicePolicy create(String clazzName) {
      try {
        Class<ReplicaChoicePolicy> clazz =
            (Class<ReplicaChoicePolicy>) Class.forName(clazzName);
        return CommonUtils.createNewClassInstance(clazz, new Class[] {}, new Object[] {});
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Given a set of workers that contain a same block, and pick one up.
   * @param workers a set of workers
   * @return one of worker
   */
  MasterWorkerInfo pickUpWorker(List<MasterWorkerInfo> workers);
}
