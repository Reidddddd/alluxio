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

package alluxio.util.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DynamicShiftQueue<T> {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicShiftQueue.class);

  private static final int CORE_NUM = Runtime.getRuntime().availableProcessors();
  private static final int HALF_CORE = CORE_NUM / 2;

  private DynamicQueue<T> firstQueue;
  private DynamicQueue<T> secondQueue;

  public DynamicShiftQueue() {
    this(HALF_CORE, CORE_NUM - HALF_CORE);
  }

  public DynamicShiftQueue(int capacityA, int capacityB) {
    Lock lock = new ReentrantLock();
    Condition cond = lock.newCondition();
    firstQueue = new DynamicQueue<T>(capacityA, lock, cond);
    secondQueue = new DynamicQueue<T>(capacityB, lock, cond);
    firstQueue.setBackupQueue(secondQueue).setQueueName(QUEUE.A);
    secondQueue.setBackupQueue(firstQueue).setQueueName(QUEUE.B);
    LOG.info("Capacity for queue A is {}, for queue B is {}.", capacityA, capacityB);
  }

  public BlockingQueue<T> getDynamicQueueA() {
    return firstQueue;
  }

  public BlockingQueue<T> getDynamicQueueB() {
    return secondQueue;
  }

  enum QUEUE {
    A, B
  }

  class DynamicQueue<T> extends LinkedBlockingQueue<T> {
    BlockingQueue<T> backupQueue;
    QUEUE name;

    final Lock lock;
    final Condition notEmpty;

    DynamicQueue(int capacity, Lock lock, Condition notEmpty) {
      super(capacity);
      this.lock = lock;
      this.notEmpty = notEmpty;
    }

    public DynamicQueue setBackupQueue(BlockingQueue queue) {
      this.backupQueue = queue;
      return this;
    }

    public DynamicQueue setQueueName(QUEUE name) {
      this.name = name;
      return this;
    }

    @Override
    public boolean offer(T t) {
      lock.lock();
      boolean success;
      try {
        success = super.offer(t);
        if (success) {
          LOG.info("New item is offered in queue {}", this.toString());
          notEmpty.signal();
          return true;
        }
      } finally {
        lock.unlock();
      }
      return backupQueue.offer(t);
    }

    @Override
    public T take() throws InterruptedException {
      lock.lockInterruptibly();
      try {
        while (true) {
          T item = this.poll();
          if (item == null) {
            item = backupQueue.poll();
            if (item == null) {
              LOG.info("Self queue {} is waiting.", this.toString());
              notEmpty.await();
            } else {
              LOG.info("Taked an item from backup queue {}", backupQueue.toString());
              return item;
            }
          } else {
            LOG.info("Taked an item from self queue {}.", this.toString());
            return item;
          }
        }
      } finally {
        lock.unlock();
      }
    }

    @Override
    public T poll(long timeout, TimeUnit unit) throws InterruptedException {
      long nano = unit.toNanos(timeout);
      lock.lockInterruptibly();
      try {
        while (true) {
          T item = this.poll();
          if (item == null) {
            item = backupQueue.poll();
            if (item == null) {
              if (nano <= 0) {
                return null;
              } else {
                LOG.info("Self queue {} is waiting for {}nns.", this.toString(), nano);
                notEmpty.awaitNanos(nano);
              }
            } else {
              LOG.info("Taked an item from backup queue {}", backupQueue.toString());
              return item;
            }
          } else {
            LOG.info("Taked an item from self queue {}.", this.toString());
            return item;
          }
        }
      } finally {
        lock.unlock();
      }
    }

    @Override
    public String toString() {
      return name.toString();
    }
  }
}
