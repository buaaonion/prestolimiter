package com.lc.prestolimiter.Producer;

import com.alibaba.fastjson.JSON;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.lc.prestolimiter.common.NativeProperties;
import com.lc.prestolimiter.common.QueryType;
import com.lc.prestolimiter.common.RegisterObject;
import com.lc.prestolimiter.common.Switchable;
import com.lc.prestolimiter.zookeeper.common.PathChangeWatcher;
import com.lc.prestolimiter.zookeeper.impl.ProducerConsumePathListener;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerService implements Switchable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class.getName());

    private CuratorFramework client;
    private Cache<String, RegisterObject> waitExecuteQueryCache;
    private PathChangeWatcher pathChangeWatcher;

    public ProducerService(CuratorFramework client) {
        this.client = client;
        waitExecuteQueryCache = CacheBuilder.newBuilder().concurrencyLevel(4)
            .expireAfterWrite(NativeProperties.getProducerRegisterMaxExpireMills(),
                TimeUnit.MILLISECONDS)
            .maximumSize(200000).build();
    }

    /**
     * ss
     * 等待当前的注册节点被消费者移动到执行目录.
     */
    public String getExecutePermission(int priority, QueryType queryType, long waitTimeoutMills) {
        long startTime = System.currentTimeMillis();
        RegisterObject registerObject = new RegisterObject(priority, queryType,
            startTime + waitTimeoutMills);
        String node;
        try {
            node = this.client.create()
                .forPath(NativeProperties.getProducerRegisterPath(),
                    JSON.toJSONBytes(registerObject));
        } catch (Exception e) {
            LOGGER.error("get execute permission register node error!", e);
            return null;
        }
        waitExecuteQueryCache
            .put(node, registerObject); //此处原则上有可能发生还没有开始wait, 就被唤醒，但是与zk交互的网络延时导致这种情况基本不会发生.
        synchronized (registerObject) {
            try {
                registerObject.wait(waitTimeoutMills);
                return node;
            } catch (InterruptedException e) {
                LOGGER.error("get execute permission wait error!", e);
                return null;
            }
        }
    }

    /**
     * 从执行目录删除节点.
     */
    public void deleteExecutePermission(String path) {
        try {
            Collection<CuratorTransactionResult> result = this.client.inTransaction().check()
                .forPath(path).and().delete().forPath(path).and().commit();
            LOGGER.info("delete execute permition result:", result);
        } catch (Exception e) {
            //如果因为其他原因导致失败，等待节点超时.
            LOGGER.error(String.format("delete execute permition error! the info is [%s]"),
                e.getMessage());
        }
    }

    /**
     * 生产者开始监听执行目录.
     */
    @Override
    public boolean start() {
        pathChangeWatcher = new PathChangeWatcher(client, NativeProperties.getProducerConsumePath(),
            new ProducerConsumePathListener(waitExecuteQueryCache));
        try {
            pathChangeWatcher.start();
            return true;
        } catch (Exception e) {
            LOGGER.error("presto limiter producer listen consume path error!", e);
            return false;
        }
    }

    @Override
    public boolean stop() {
        if (pathChangeWatcher != null) {
            pathChangeWatcher.stop();
        }
        waitExecuteQueryCache.asMap().values()
            .forEach(registerObject -> {
                try {
                    client.delete().forPath(NativeProperties.getProducerRegisterPath()
                        + "/" + registerObject.getNode());
                } catch (Exception e) {
                    try {
                        client.delete().forPath(NativeProperties.getProducerConsumePath()
                            + "/" + registerObject.getNode());
                    } catch (Exception e1) {
                    }
                }
            });
        return true;
    }
}
