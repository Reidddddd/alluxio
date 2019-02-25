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

package alluxio.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;

/**
 * A scheduler mainly schedules threads running at off-peak time.
 * It can schedule many jobs, but the best practice is to make every jobs no overlap.
 * For example, it runs BlockBalancer from 10.pm ~ 23pm, runs ReplicationManager from 8.00pm ~ 9.00pm.
 * Because threads are scheduled sequentially, if there are overlaps, the latter will be delayed.
 * So far, it doesn't support minute level.
 */
public class OffPeakTimer {
  private static final Logger LOG = LoggerFactory.getLogger(OffPeakTimer.class);

  /** Start timer*/
  private final Timer mStartTimer = new Timer();
  /** Stop timer*/
  private final Timer mStopTimer = new Timer();

  /**
   * Schedule timer.
   * @param task Timer task
   */
  public void trigger(TimerExecutor task) {
    mStartTimer.schedule(task.getStarter(), task.getStartDate(), task.getPeriod());
    mStopTimer.schedule(task.getStopper(), task.getStopDate(), task.getPeriod());
  }

  /**
   * Close all timer task.
   */
  public void close() {
    mStartTimer.cancel();
    mStopTimer.cancel();
  }

  public enum OffPeakUnit {
    HOUR, MINUTE, SECOND
  }
}
