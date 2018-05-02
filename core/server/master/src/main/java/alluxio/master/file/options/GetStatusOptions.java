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
import alluxio.thrift.GetStatusTOptions;
import alluxio.wire.LoadMetadataType;

import alluxio.wire.TtlAction;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for getStatus.
 */
@NotThreadSafe
public final class GetStatusOptions {
  private LoadMetadataType mLoadMetadataType;
  private long mTtl;
  private TtlAction mAction;

  /**
   * @return the default {@link GetStatusOptions}
   */
  public static GetStatusOptions defaults() {
    return new GetStatusOptions();
  }

  private GetStatusOptions() {
    mLoadMetadataType = LoadMetadataType.Once;
    mTtl = Constants.NO_TTL;
    mAction = TtlAction.DELETE;
  }

  /**
   * Create an instance of {@link GetStatusOptions} from a {@link GetStatusTOptions}.
   *
   * @param options the thrift representation of getFileInfo options
   */
  public GetStatusOptions(GetStatusTOptions options) {
    mLoadMetadataType = LoadMetadataType.Once;
    if (options.isSetLoadMetadataType()) {
      mLoadMetadataType = LoadMetadataType.fromThrift(options.getLoadMetadataType());
    }
    mTtl = options.getTtl();
    mAction = TtlAction.fromThrift(options.getTtlAction());
  }

  /**
   * @return the load metadata type
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
    return mAction;
  }

  /**
   * Sets the {@link GetStatusOptions#mLoadMetadataType}.
   *
   * @param loadMetadataType the load metadata type
   * @return the updated options
   */
  public GetStatusOptions setLoadMetadataType(LoadMetadataType loadMetadataType) {
    mLoadMetadataType = loadMetadataType;
    return this;
  }

  /**
   * Sets the {@link GetStatusOptions#mTtl}.
   *
   * @param ttl time to live
   * @return the updated options
   */
  public GetStatusOptions setTtl(long ttl) {
    mTtl = ttl;
    return this;
  }

  /**
   * Sets the {@link GetStatusOptions#mAction}.
   *
   * @param ttlAction action after ttl expired
   * @return the updated options
   */
  public GetStatusOptions setTtlAction(TtlAction ttlAction) {
    mAction = ttlAction;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GetStatusOptions)) {
      return false;
    }
    GetStatusOptions that = (GetStatusOptions) o;
    return Objects.equal(mLoadMetadataType, that.mLoadMetadataType) &&
        Objects.equal(mTtl, that.mTtl) &&
        Objects.equal(mAction, that.mAction);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mLoadMetadataType, mTtl, mAction);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("loadMetadataType", mLoadMetadataType.toString())
        .add("ttl", mTtl)
        .add("ttlAction", mAction.toString())
        .toString();
  }
}
