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

package alluxio.worker.netty;

import alluxio.proto.dataserver.Protocol;
import alluxio.worker.block.io.BlockReader;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Context of {@link BlockReadRequest}.
 */
@NotThreadSafe
public final class BlockReadRequestContext extends ReadRequestContext<BlockReadRequest> {
  private BlockReader mBlockReader;
  private boolean mBlockDeleted = false;

  /**
   * @param request read request in proto
   */
  public BlockReadRequestContext(Protocol.ReadRequest request) {
    super(new BlockReadRequest(request));
  }

  /**
   * @return block reader
   */
  @Nullable
  public BlockReader getBlockReader() {
    return mBlockReader;
  }

  /**
   * @param blockReader block reader to set
   */
  public void setBlockReader(BlockReader blockReader) {
    mBlockReader = blockReader;
  }

  /**
   * Set block is deleted, this is used in block transfer.
   */
  public void setBlockDeleted() {
    mBlockDeleted = true;
  }

  /**
   * If block is deleted.
   * @return true if it is, false otherwise
   */
  public boolean blockDeleted() {
    return mBlockDeleted;
  }
}
