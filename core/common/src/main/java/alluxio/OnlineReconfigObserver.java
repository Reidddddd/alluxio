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

/**
 * Every class that wants to observe changes in configuration properties, must implement this
 * interface, and also register itself with the <code>ConfigurationManager</code> object.
 */
public interface OnlineReconfigObserver {

  /**
   * Whether configurations are different between those original.
   * @return true if they are different, false otherwise
   */
  boolean needUpdate();

  /**
   * This method is called after {@link OnlineReconfigObserver#needUpdate()} return true.
   * An instance should do reconfiguration works itself in this method.
   */
  void onConfigurationChange();

}
