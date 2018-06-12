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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.OnlineReconfigManager;
import alluxio.OnlineReconfigObserver;
import alluxio.PropertyKey;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * Host manager is responsible for serving hosts.
 */
@NotThreadSafe
public class HostManager implements OnlineReconfigObserver {
  private static final Logger LOG = LoggerFactory.getLogger(HostManager.class);

  /** A set of name of serving hosts. */
  private Set<String> mWorkersLocation;
  /** This set is used only at starts up to let admin know which worker isn't up. */
  private Set<String> mCheckStartup;

  public HostManager() {
    mWorkersLocation = new HashSet<>();
    mCheckStartup = new HashSet<>();
    initialize();
    OnlineReconfigManager.getInstance().registerObserver(this);
  }

  private void initialize() {
    String confLocations = Configuration.get(PropertyKey.SITE_CONF_DIR);
    String[] locations = confLocations.split(",");
    for (String location : locations) {
      File file = new File(PathUtils.concatPath(location, Constants.INCLUDE_WORKERS_FILE));
      if (!file.exists()) {
        continue;
      }

      try (BufferedReader br = new BufferedReader(
          new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8))) {
        String hostname;
        while ((hostname = br.readLine()) != null) {
          hostname = hostname.trim();
          if (hostname.isEmpty() || hostname.startsWith("#")) {
            continue;
          }
          mWorkersLocation.add(hostname);
          mCheckStartup.add(hostname);
        }
      } catch (IOException e) {
        LOG.error("Exception occurred while HostManager is reading host file, exception is {}.",
            e.getMessage());
      }
      break;
    }
  }

  /**
   * @param hostname host name of a worker
   */
  public void workerIsUp(String hostname) {
    // This may concurrently be called by registration threads.
    synchronized (mCheckStartup) {
      if (mCheckStartup.contains(hostname)) {
        mCheckStartup.remove(hostname);
      }
      LOG.info("Remaining hosts are: {}.", mCheckStartup.toString());
      if (mCheckStartup.isEmpty()) {
        mCheckStartup.clear();
      }
    }
  }

  @Override
  public boolean needUpdate() {
    mWorkersLocation.clear();
    return true;
  }

  @Override
  public void onConfigurationChange() {
    initialize();
  }
}
