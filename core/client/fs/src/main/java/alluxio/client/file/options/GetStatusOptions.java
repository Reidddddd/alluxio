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

package alluxio.client.file.options;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.thrift.GetStatusTOptions;
import alluxio.wire.LoadMetadataType;

import alluxio.wire.TtlAction;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for getting the status of a path.
 */
@PublicApi
@NotThreadSafe
@JsonInclude(Include.NON_EMPTY)
public final class GetStatusOptions {
  private LoadMetadataType mLoadMetadataType;
  private long mTtl;
  private TtlAction mTtlAction;

  /**
   * @return the default {@link GetStatusOptions}
   */
  public static GetStatusOptions defaults() {
    return new GetStatusOptions();
  }

  private GetStatusOptions() {
    mLoadMetadataType =
        Configuration.getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class);
    mTtl = Configuration.getLong(PropertyKey.USER_FILE_READ_CACHE_TTL_MS);
    mTtlAction =
        Configuration.getEnum(PropertyKey.USER_FILE_READ_CACHE_TTL_EXPIRED_ACTION, TtlAction.class);
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
    return mTtlAction;
  }

  /**
   * @param loadMetadataType the loadMetataType
   * @return the updated options
   */
  public GetStatusOptions setLoadMetadataType(LoadMetadataType loadMetadataType) {
    mLoadMetadataType = loadMetadataType;
    return this;
  }

  /**
   * @param ttl time to live
   */
  public void setTtl(long ttl) {
    mTtl = ttl;
  }

  /**
   * @param ttlAction action after ttl expired
   */
  public void setTtlAction(TtlAction ttlAction) {
    mTtlAction = ttlAction;
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
        Objects.equal(mTtlAction, that.mTtlAction);
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

  /**
   * @return thrift representation of the options
   */
  public GetStatusTOptions toThrift() {
    GetStatusTOptions options = new GetStatusTOptions();
    options.setLoadMetadataType(LoadMetadataType.toThrift(mLoadMetadataType));
    options.setTtl(mTtl);
    options.setTtlAction(TtlAction.toThrift(mTtlAction));
    return options;
  }
}
