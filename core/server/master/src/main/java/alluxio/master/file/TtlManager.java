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

package alluxio.master.file;

import alluxio.Configuration;
import alluxio.OnlineReconfigManager;
import alluxio.OnlineReconfigObserver;
import alluxio.PropertyKey;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.wire.TtlAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TtlManager implements OnlineReconfigObserver {
  private static final Logger LOG = LoggerFactory.getLogger(TtlManager.class);

  private final TtlAction mAction = TtlAction.FREE;
  private long mTimeToLive;

  public TtlManager() {
    mTimeToLive = Configuration.getMs(PropertyKey.MASTER_TTL_MANAGER_TIME_TO_LIVE);
    OnlineReconfigManager.getInstance().registerObserver(this);
  }

  /**
   * Inject inode's ttl properties to CreateFileOptions
   * @param options a CreateFileOptions
   * @return ttled CreateFileOptions
   */
  public CreateFileOptions injectInodeTtl(CreateFileOptions options) {
    options.setTtl(mTimeToLive);
    options.setTtlAction(mAction);
    return options;
  }

  @Override
  public boolean needUpdate() {
    if (mTimeToLive != Configuration.getMs(PropertyKey.MASTER_TTL_MANAGER_TIME_TO_LIVE)) {
      LOG.info("Updating {} from {} to {}",
        PropertyKey.MASTER_TTL_MANAGER_TIME_TO_LIVE,
        mTimeToLive,
        Configuration.getMs(PropertyKey.MASTER_TTL_MANAGER_TIME_TO_LIVE));
      return true;
    }
    return false;
  }

  @Override
  public void onConfigurationChange() {
    mTimeToLive = Configuration.getMs(PropertyKey.MASTER_TTL_MANAGER_TIME_TO_LIVE);
  }
}
