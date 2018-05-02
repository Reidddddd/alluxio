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
import alluxio.thrift.ListStatusTOptions;
import alluxio.wire.LoadMetadataType;

import alluxio.wire.TtlAction;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for list status.
 */
@NotThreadSafe
public final class ListStatusOptions {
  private LoadMetadataType mLoadMetadataType;
  private long mTtl;
  private TtlAction mTtlAction;

  /**
   * @return the default {@link ListStatusOptions}
   */
  public static ListStatusOptions defaults() {
    return new ListStatusOptions();
  }

  private ListStatusOptions() {
    mLoadMetadataType = LoadMetadataType.Once;
    mTtl = Constants.NO_TTL;
    mTtlAction = TtlAction.DELETE;
  }

  /**
   * Create an instance of {@link ListStatusOptions} from a {@link ListStatusTOptions}.
   *
   * @param options the thrift representation of list status options
   */
  public ListStatusOptions(ListStatusTOptions options) {
    mLoadMetadataType = LoadMetadataType.Once;
    if (options.isSetLoadMetadataType()) {
      mLoadMetadataType = LoadMetadataType.fromThrift(options.getLoadMetadataType());
    } else if (!options.isLoadDirectChildren()) {
      mLoadMetadataType = LoadMetadataType.Never;
    }
    mTtl = options.getTtl();
    mTtlAction = TtlAction.fromThrift(options.getTtlAction());
  }

  /**
   * @return the load metadata type. It specifies whether the direct children should
   *         be loaded from UFS in different scenarios.
   */
  public LoadMetadataType getLoadMetadataType() {
    return mLoadMetadataType;
  }

  /**
   * @return time to live
   */
  public long getTtl() {
    return mTtl;
  }

  /**
   * @return action after ttl expired
   */
  public TtlAction getTtlAction() {
    return mTtlAction;
  }

  /**
   * Sets the {@link ListStatusOptions#mLoadMetadataType}.
   *
   * @param loadMetadataType the load metadata type
   * @return the updated options
   */
  public ListStatusOptions setLoadMetadataType(LoadMetadataType loadMetadataType) {
    mLoadMetadataType = loadMetadataType;
    return this;
  }

  /**
   * Sets the {@link ListStatusOptions#mTtl}.
   *
   * @param ttl time to live
   * @return the updated options
   */
  public void setTtl(long ttl) {
    mTtl = ttl;
  }

  /**
   * Sets the {@link ListStatusOptions#mTtlAction}.
   *
   * @param ttlAction action after ttl expired
   * @return the updated options
   */
  public void setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ListStatusOptions)) {
      return false;
    }
    ListStatusOptions that = (ListStatusOptions) o;
    return Objects.equal(mLoadMetadataType, that.mLoadMetadataType)
        && Objects.equal(mTtl, that.mTtl)
        && Objects.equal(mTtlAction, that.mTtlAction);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLoadMetadataType, mTtl, mTtlAction);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("loadMetadataType", mLoadMetadataType.toString())
        .add("ttl", mTtl)
        .add("ttlAction", mTtlAction.toString())
        .toString();
  }
}
