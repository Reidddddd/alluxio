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

package alluxio.master.file.options;

import alluxio.Constants;
import alluxio.underfs.UfsStatus;

import alluxio.wire.TtlAction;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.Nullable;

/**
 * Method options for loading metadata.
 */
@NotThreadSafe
public final class LoadMetadataOptions {
  private boolean mCreateAncestors;
  private boolean mLoadDirectChildren;
  private UfsStatus mUfsStatus;
  private long mTtl;
  private TtlAction mAction;

  /**
   * @return the default {@link LoadMetadataOptions}
   */
  public static LoadMetadataOptions defaults() {
    return new LoadMetadataOptions();
  }

  private LoadMetadataOptions() {
    mCreateAncestors = false;
    mLoadDirectChildren = false;
    mUfsStatus = null;
    mTtl = Constants.NO_TTL;
    mAction = TtlAction.FREE;
  }

  /**
   * @return null if unknown, else the status of UFS path for which loading metadata
   */
  @Nullable
  public UfsStatus getUfsStatus() {
    return mUfsStatus;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isCreateAncestors() {
    return mCreateAncestors;
  }

  /**
   * @return the load direct children flag. It specifies whether the direct children should
   * be loaded.
   */
  public boolean isLoadDirectChildren() {
    return mLoadDirectChildren;
  }

  /**
   * @return the time to live of an inode
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return the action after ttl is expired
   */
  public TtlAction getTtlAction() {
    return mAction;
  }

  /**
   * Sets the recursive flag.
   *
   * @param createAncestors the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public LoadMetadataOptions setCreateAncestors(boolean createAncestors) {
    mCreateAncestors = createAncestors;
    return this;
  }

  /**
   * Sets the load direct children flag.
   *
   * @param loadDirectChildren the load direct children flag. It specifies whether the direct
   *                           children should be loaded.
   * @return the updated object
   */
  public LoadMetadataOptions setLoadDirectChildren(boolean loadDirectChildren) {
    mLoadDirectChildren = loadDirectChildren;
    return this;
  }

  /**
   * Sets the UFS status of path.
   *
   * @param status UFS status of path
   * @return the updated object
   */
  public LoadMetadataOptions setUfsStatus(UfsStatus status) {
    mUfsStatus = status;
    return this;
  }

  /**
   * Sets the ttl of a path
   *
   * @param ttl
   * @return the updated object
   */
  public LoadMetadataOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * Sets the action after ttl expired
   *
   * @param ttlAction
   * @return the updated object
   */
  public LoadMetadataOptions setTtlAction(TtlAction ttlAction) {
    mAction = ttlAction;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LoadMetadataOptions)) {
      return false;
    }
    LoadMetadataOptions that = (LoadMetadataOptions) o;
    return Objects.equal(mCreateAncestors, that.mCreateAncestors)
        && Objects.equal(mLoadDirectChildren, that.mLoadDirectChildren)
        && Objects.equal(mUfsStatus, that.mUfsStatus)
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mAction, that.mAction);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCreateAncestors, mLoadDirectChildren, mUfsStatus, mTtl, mAction);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("createAncestors", mCreateAncestors)
        .add("loadDirectChildren", mLoadDirectChildren)
        .add("ufsStatus", mUfsStatus)
        .add("ttl", mTtl)
        .add("ttlAction", mAction)
        .toString();
  }
}
