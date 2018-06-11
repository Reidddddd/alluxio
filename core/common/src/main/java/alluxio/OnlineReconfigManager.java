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

package alluxio;

import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * OnlineReconfigManager is responsible for those classed that would like to get notified when
 * configuration is reloaded, which lets you update configuration properties on the fly, without
 * restarting cluster.
 *
 * If you want a class to support this feature, do the following:
 * 1. Implement the {@link OnlineReconfigObserver} interface, this would require you to implement
 *    {@link OnlineReconfigObserver#needUpdate()}
 *    and {@link OnlineReconfigObserver#onConfigurationChange()} method. The former method is used
 *    to judge whether configuration is changed, if so it will call the latter method.
 * 2. Register the instance of the class with {@link OnlineReconfigManager}, using method
 *    {@link OnlineReconfigManager#registerObserver(OnlineReconfigObserver)}.
 * 3. Deregister the instance of the class with {@link OnlineReconfigManager}, using method
 *    {@link OnlineReconfigManager#deregisterObserver(OnlineReconfigObserver)}.
 */

@ThreadSafe
public class OnlineReconfigManager {
  private static final Logger LOG = LoggerFactory.getLogger(OnlineReconfigManager.class);

  /**
   * The set of OnlineReconfig Observers. These classes would like to get notified when
   * configuration is reloaded.
   */
  private final Set<OnlineReconfigObserver> mOnlineReconfigObservers =
      new HashSet<>();

  /** Singleton instance of OnlineReconfigManager */
  private OnlineReconfigManager() {}
  private static class Singleton {
    private static final OnlineReconfigManager INSTANCE = new OnlineReconfigManager();
  }

  /**
   * Get OnlineReconfigManager instance.
   * @return OnlineReconfigManager
   */
  public static OnlineReconfigManager getInstance() {
    return Singleton.INSTANCE;
  }

  /**
   * Register an observer class.
   * @param observer a class implements OnlineReconfigObserver
   */
  public void registerObserver(OnlineReconfigObserver observer) {
    synchronized (mOnlineReconfigObservers) {
      mOnlineReconfigObservers.add(observer);
    }
    LOG.info("{} supports on-the-fly reconfiguration.", observer.getClass().getSimpleName());
  }

  /**
   * Deregister an observer class.
   * @param observer a class implements OnlineReconfigObserver
   */
  public void deregisterObserver(OnlineReconfigObserver observer) {
    synchronized (mOnlineReconfigObservers) {
      mOnlineReconfigObservers.remove(observer);
    }
  }

  /**
   * Notify all the observers that configuration get changed.
   */
  public void notifyAllObservers() {
    synchronized (mOnlineReconfigObservers) {
      for (OnlineReconfigObserver observer : mOnlineReconfigObservers) {
        try {
          if (observer.needUpdate()) {
            LOG.info("Start reconfiguring {}.", observer.getClass().getSimpleName());
            observer.onConfigurationChange();
          }
        } catch (Throwable t) {
          LOG.error("Unable to update {} for reasons {}.", observer, t.getCause());
        }
      }
    }
  }
}
