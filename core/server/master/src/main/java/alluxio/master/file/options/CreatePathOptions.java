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
import alluxio.security.authorization.Mode;

import alluxio.wire.TtlAction;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a path.
 *
 * @param <T> the type of the object to create
 */
@NotThreadSafe
public abstract class CreatePathOptions<T> {
  protected boolean mMountPoint;
  protected long mOperationTimeMs;
  protected String mOwner;
  protected String mGroup;
  protected Mode mMode;
  protected boolean mPersisted;
  // TODO(peis): Rename this to mCreateAncestors.
  protected boolean mRecursive;
  protected boolean mMetadataLoad;
  protected long mTtl;
  protected TtlAction mTtlAction;

  protected CreatePathOptions() {
    mMountPoint = false;
    mOperationTimeMs = System.currentTimeMillis();
    mOwner = "";
    mGroup = "";
    mMode = Mode.defaults();
    mPersisted = false;
    mRecursive = false;
    mMetadataLoad = false;
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
  }

  protected abstract T getThis();

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @return the mount point flag; it specifies whether the object to create is a mount point
   */
  public boolean isMountPoint() {
    return mMountPoint;
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the group
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the mode
   */
  public Mode getMode() {
    return mMode;
  }

  /**
   * @return the persisted flag; it specifies whether the object to create is persisted in UFS
   */
  public boolean isPersisted() {
    return mPersisted;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the metadataLoad flag; if true, the create path is a result of a metadata load
   */
  public boolean isMetadataLoad() {
    return mMetadataLoad;
  }

  /**
   * @return the TTL (time to live) value; it identifies duration (in seconds) the created file
   *         should be kept around before it is automatically deleted
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return action the {@link TtlAction} after TTL expired
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
  }

  /**
   * @param mountPoint the mount point flag to use; it specifies whether the object to create is
   *        a mount point
   * @return the updated options object
   */
  public T setMountPoint(boolean mountPoint) {
    mMountPoint = mountPoint;
    return getThis();
  }

  /**
   * @param operationTimeMs the operation time to use
   * @return the updated options object
   */
  public T setOperationTimeMs(long operationTimeMs) {
    mOperationTimeMs = operationTimeMs;
    return getThis();
  }

  /**
   * @param owner the owner to use
   * @return the updated options object
   */
  public T setOwner(String owner) {
    mOwner = owner;
    return getThis();
  }

  /**
   * @param group the group to use
   * @return the updated options object
   */
  public T setGroup(String group) {
    mGroup = group;
    return getThis();
  }

  /**
   * @param mode the mode to use
   * @return the updated options object
   */
  public T setMode(Mode mode) {
    mMode = mode;
    return getThis();
  }

  /**
   * @param persisted the persisted flag to use; it specifies whether the object to create is
   *        persisted in UFS
   * @return the updated options object
   */
  public T setPersisted(boolean persisted) {
    mPersisted = persisted;
    return getThis();
  }

  /**
   * @param recursive the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public T setRecursive(boolean recursive) {
    mRecursive = recursive;
    return getThis();
  }

  /**
   * @param metadataLoad the flag value to use; if true, the create path is a result of a
   *                     metadata load
   * @return the updated options object
   */
  public T setMetadataLoad(boolean metadataLoad) {
    mMetadataLoad = metadataLoad;
    return getThis();
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted
   * @return the updated options object
   */
  public T setTtl(long ttl) {
    mTtl = ttl;
    return getThis();
  }

  /**
   * @param ttlAction the {@link TtlAction}; It informs the action to take when Ttl is expired;
   * @return the updated options object
   */
  public T setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
    return getThis();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreatePathOptions)) {
      return false;
    }
    CreatePathOptions<?> that = (CreatePathOptions<?>) o;
    return Objects.equal(mMountPoint, that.mMountPoint)
        && Objects.equal(mOwner, that.mOwner)
        && Objects.equal(mGroup, that.mGroup)
        && Objects.equal(mMode, that.mMode)
        && Objects.equal(mPersisted, that.mPersisted)
        && Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mMetadataLoad, that.mMetadataLoad)
        && mOperationTimeMs == that.mOperationTimeMs
        && mTtl == that.mTtl
        && Objects.equal(mTtlAction, that.mTtlAction);
  }

  @Override
  public int hashCode() {
    return Objects
        .hashCode(mMountPoint, mOwner, mGroup, mMode, mPersisted, mRecursive, mMetadataLoad,
            mOperationTimeMs, mTtl, mTtlAction);
  }

  protected Objects.ToStringHelper toStringHelper() {
    return Objects.toStringHelper(this)
        .add("mountPoint", mMountPoint)
        .add("operationTimeMs", mOperationTimeMs)
        .add("owner", mOwner)
        .add("group", mGroup)
        .add("mode", mMode)
        .add("persisted", mPersisted)
        .add("recursive", mRecursive)
        .add("metadataLoad", mMetadataLoad)
        .add("ttl", mTtl)
        .add("ttlAction", mTtlAction);
  }
}
