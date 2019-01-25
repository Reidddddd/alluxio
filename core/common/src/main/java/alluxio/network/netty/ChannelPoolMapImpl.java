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

package alluxio.network.netty;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.network.protocol.RPCMessage;
import alluxio.util.ThreadFactoryUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ChannelPoolMapImpl extends AbstractChannelPoolMap<SocketAddress, FixedChannelPool>  {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelPoolMapImpl.class);

  private static final ConcurrentMap<SocketAddress, Bootstrap> map = PlatformDependent.newConcurrentHashMap();

  private int mPoolSizeOfAddr;

  public ChannelPoolMapImpl() {
    mPoolSizeOfAddr = Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_SIZE_MAX);
    LOG.info("Size of pool for each address {}.", mPoolSizeOfAddr);
  }

  public void put(SocketAddress addr, Bootstrap bs) {
    Bootstrap oldbs = map.putIfAbsent(addr, bs);
    if (oldbs == null) {
      LOG.info("New connection to address {}.", addr);
    }
  }

  public long allSize() {
    return (long) (size() * mPoolSizeOfAddr);
  }

  @Override
  protected FixedChannelPool newPool(SocketAddress address) {
    LOG.info("New pool to address {}.", address.toString());
    return new FixedChannelPool(map.get(address), new ChannelHandler(), mPoolSizeOfAddr);
  }

  private static class ChannelHandler implements ChannelPoolHandler {
    static final long timeoutMs = Configuration.getMs(PropertyKey.NETWORK_NETTY_HEARTBEAT_TIMEOUT_MS);
    static final long gcTime = Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_CHANNEL_POOL_GC_THRESHOLD_MS);
    static final long heartbeatPeriodMs = Math.max(timeoutMs / 10, 1);

    static final ConcurrentMap<Channel, Long> idleChannels = PlatformDependent.newConcurrentHashMap();
    static final ScheduledExecutorService service =
      Executors.newScheduledThreadPool(1,
        ThreadFactoryUtils.build("Idle Channel Cleaner", true));

    static {
      service.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
          LOG.info("Start checking idle channel!");
          long currentTime = System.currentTimeMillis();
          Iterator<Map.Entry<Channel, Long>> it = idleChannels.entrySet().iterator();
          while (it.hasNext()) {
            Map.Entry<Channel, Long> channel = it.next();
            long lastExeTime = channel.getValue();
            if (currentTime - lastExeTime > gcTime) {
              Channel c = channel.getKey();
              LOG.info("{} is idle for more than {}ms, start cleaning.", c, gcTime);
              c.close().syncUninterruptibly();
              it.remove();
            }
          }
        }
      }, 0, gcTime / 2, TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelReleased(Channel channel) {
      LOG.debug("Channel {} is released.", channel);
      idleChannels.put(channel, System.currentTimeMillis());
    }

    @Override
    public void channelAcquired(Channel channel) {
      LOG.debug("Channel {} is acquired.", channel);
      idleChannels.remove(channel);
    }

    @Override
    public void channelCreated(Channel channel) {
      ChannelPipeline pipeline = channel.pipeline();
      // After 10 missed heartbeat attempts and no write activity, the server will close the channel.
      pipeline.addLast(RPCMessage.createFrameDecoder());
      pipeline.addLast(NettyClient.ENCODER);
      pipeline.addLast(NettyClient.DECODER);
      pipeline.addLast(new IdleStateHandler(0, heartbeatPeriodMs, 0, TimeUnit.MILLISECONDS));
      pipeline.addLast(new IdleWriteHandler());
      LOG.info("Channel {} is created.", channel);
    }
  }
}
