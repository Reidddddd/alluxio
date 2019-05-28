
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

import java.util.Calendar;
import java.util.Date;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * A timer task runs periodically. It should contain a start date and a stop date.
 * At start|stop date, timer will run starter|stopper.
 */
public abstract class TimerExecutor {

  private final Calendar mCalendar = Calendar.getInstance();

  /* Time Unit */
  private static final long T_SECOND = 1000;
  private static final long T_MINUTE = T_SECOND * 60;
  private static final long T_HOUR = T_MINUTE * 60;
  private static final long T_DAY = T_HOUR * 24;

  /**
   * Start date
   */
  private Date mStartDate;
  /**
   * Stop date
   */
  private Date mStopDate;
  /**
   * Running period
   */
  private long mPeriod;

  public TimerExecutor(OffPeakTimer.OffPeakUnit unit, int startTime, int stopTime) {
    checkTimeUnitLegal(unit, startTime, stopTime);

    if (unit == OffPeakTimer.OffPeakUnit.SECOND) {
      mPeriod = T_MINUTE;
      mCalendar.set(Calendar.SECOND, startTime);
      mStartDate = mCalendar.getTime();
      mCalendar.set(Calendar.SECOND, stopTime);
      mStopDate = mCalendar.getTime();
    } else if (unit == OffPeakTimer.OffPeakUnit.MINUTE) {
      mPeriod = T_HOUR;
      mCalendar.set(Calendar.MINUTE, startTime);
      mStartDate = mCalendar.getTime();
      mCalendar.set(Calendar.MINUTE, stopTime);
      mStopDate = mCalendar.getTime();
    } else if (unit == OffPeakTimer.OffPeakUnit.HOUR) {
      mPeriod = T_DAY;
      mCalendar.set(Calendar.HOUR_OF_DAY, startTime);
      mStartDate = mCalendar.getTime();
      mCalendar.set(Calendar.HOUR_OF_DAY, stopTime);
      mStopDate = mCalendar.getTime();
    }
  }

  private void checkTimeUnitLegal(OffPeakTimer.OffPeakUnit unit, int startTime, int stopTime) {
    int lowerBound = 0;
    int upperBound = unit == OffPeakTimer.OffPeakUnit.HOUR ? 24 :
                     unit == OffPeakTimer.OffPeakUnit.MINUTE ? 60 : 60;
    if (startTime >= upperBound || startTime < lowerBound) {
      throw new IllegalArgumentException("start_time should between [0, " + upperBound + "]");
    }
    if (stopTime >= upperBound || stopTime < lowerBound) {
      throw new IllegalArgumentException("stop_time should between [0, " + upperBound + "]");
    }
    if (startTime >= stopTime) {
      throw new IllegalArgumentException("start_time should < stop_time");
    }
  }

  /**
   * Action to start timer.
   *
   * @return a start task
   */
  TimerTask getStarter() {
    return new TimerTask() {
      Starter starter = new Starter(TimerExecutor.this);

      @Override public void run() {
        starter.start();
      }
    };
  }

  /**
   * Specific time to start the timer.
   *
   * @return a start date
   */
  Date getStartDate() {
    return mStartDate;
  }

  /**
   * Action to stop timer.
   *
   * @return a stop task
   */
  TimerTask getStopper() {
    return new TimerTask() {
      Stopper stopper = new Stopper(TimerExecutor.this);

      @Override public void run() {
        stopper.stop();
      }
    };
  }

  /**
   * Specific time to stop the timer.
   *
   * @return a stop date
   */
  Date getStopDate() {
    return mStopDate;
  }

  /**
   * Timer running interval, 1 day by default.
   *
   * @return interval represented in ms
   */
  long getPeriod() {
    return mPeriod;
  }

  /**
   * Start the timer task.
   */
  abstract protected void start();

  /**
   * Stop the timer task.
   */
  abstract protected void stop();

  private class Starter {
    TimerExecutor executor;

    Starter(TimerExecutor timerExecutor) {
      executor = timerExecutor;
    }

    public void start() {
      executor.start();
    }
  }

  private class Stopper {
    TimerExecutor executor;

    Stopper(TimerExecutor timerExecutor) {
      executor = timerExecutor;
    }

    public void stop() {
      executor.stop();
    }
  }
}
