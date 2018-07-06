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

package alluxio.worker.block;

import alluxio.Configuration;
import alluxio.PropertyKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Read/write lock associated with clients rather than threads. Either its read lock or write lock
 * can be released by a thread different from the one acquiring them (but supposed to be requested
 * by the same client).
 */
@ThreadSafe
public final class ClientRWLock implements ReadWriteLock {
  private static final Logger LOG = LoggerFactory.getLogger(ClientRWLock.class);

  /** Total number of permits. This value decides the max number of concurrent readers. */
  private static final int MAX_AVAILABLE =
          Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCK_READERS);
  /**
   * Uses the unfair lock to prevent a read lock that fails to release from locking the block
   * forever and thus blocking all the subsequent write access.
   * See https://alluxio.atlassian.net/browse/ALLUXIO-2636.
   */
  private final Semaphore mAvailable = new Semaphore(MAX_AVAILABLE, false);
  /** Reference count. */
  private AtomicInteger mReferences = new AtomicInteger();

  private final ReentrantReadWriteLock mRWLock = new ReentrantReadWriteLock();

  /**
   * Constructs a new {@link ClientRWLock}.
   */
  public ClientRWLock() {}

  private enum LockType {
    READ,
    WRITE
  }

  @Override
  public Lock readLock() {
    return new SessionLock(LockType.READ);
  }

  @Override
  public Lock writeLock() {
    return new SessionLock(LockType.WRITE);
  }

  /**
   * @return the reference count
   */
  public int getReferenceCount() {
    return mReferences.get();
  }

  /**
   * Increments the reference count.
   */
  public void addReference() {
    mReferences.incrementAndGet();
  }

  /**
   * Decrements the reference count.
   *
   * @return the new reference count
   */
  public int dropReference() {
    return mReferences.decrementAndGet();
  }

  private final class SessionLock implements Lock {
    private final LockType mType;
    private final Lock mLock;

    private SessionLock(LockType type) {
      mType = type;
      mLock = type == LockType.READ ? mRWLock.readLock() : mRWLock.writeLock();
    }

    @Override
    public void lock() {
      LOG.info("{} is acquiring {} lock", Thread.currentThread().getName(), mType);
      mLock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      mLock.lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
      return mLock.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
      try {
        return mLock.tryLock(time, unit);
      } catch (InterruptedException e) {
        return false;
      }
    }

    @Override
    public void unlock() {
      mLock.unlock();
      LOG.info("{} is releasing {} lock", Thread.currentThread().getName(), mType);
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException("newCondition() is not supported");
    }
  }
}
