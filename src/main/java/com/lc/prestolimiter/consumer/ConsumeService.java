package com.lc.prestolimiter.consumer;

import com.alibaba.fastjson.JSON;
import com.lc.prestolimiter.common.NativeProperties;
import com.lc.prestolimiter.common.QueryType;
import com.lc.prestolimiter.common.RegisterObject;
import com.lc.prestolimiter.common.Switchable;
import com.lc.prestolimiter.zookeeper.common.PathChangeWatcher;
import com.lc.prestolimiter.zookeeper.impl.ConsumerConsumePathListener;
import com.lc.prestolimiter.zookeeper.impl.ConsumerRegisterPathListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//消费者消费服务.
public class ConsumeService implements Switchable, Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumeService.class.getName());

    private PathChangeWatcher registerPathWatcher;
    private PathChangeWatcher consumePathWatcher;
    private AtomicInteger initialized;
    private boolean canConsume; //用于网络连接丢失的情况下暂停消费
    private final CuratorFramework client;
    private final PriorityQueue<RegisterObject> producerRegisterQueue;
    private final PriorityQueue<RegisterObject> producerConsumeQueue;
    private final Map<QueryType, Integer> queryTypeLimiterMap;
    private final Lock lock; //同时lock两个队列以及限制map.
    private final ScheduledExecutorService scheduledExecutor;

    public ConsumeService(CuratorFramework client) {
        this.client = client;
        this.producerRegisterQueue = new PriorityQueue<>();
        this.producerConsumeQueue = new PriorityQueue<>();
        this.lock = new ReentrantLock();
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        queryTypeLimiterMap = initQueryTypeLimitMap();
        initialized = new AtomicInteger(2);
    }

    @Override
    public boolean start() {
        registerPathWatcher = new PathChangeWatcher(client,
            NativeProperties.getProducerRegisterPath(),
            new ConsumerRegisterPathListener(this, initialized));
        consumePathWatcher = new PathChangeWatcher(client,
            NativeProperties.getProducerConsumePath(),
            new ConsumerConsumePathListener(this, initialized));
        try {
            registerPathWatcher.start(StartMode.POST_INITIALIZED_EVENT);
            consumePathWatcher.start(StartMode.POST_INITIALIZED_EVENT);
            scheduledExecutor.scheduleAtFixedRate(this, 0, 5, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            LOGGER.error("start consume path listen fail!", e);
            this.canConsume = false;
            return false;
        }
    }

    @Override
    public boolean stop() {
        canConsume = false;
        if (scheduledExecutor != null) {
            scheduledExecutor.shutdown();
        }
        if (registerPathWatcher != null) {
            registerPathWatcher.stop();
        }
        if (consumePathWatcher != null) {
            consumePathWatcher.stop();
        }
        return true;
    }

    //定时检查消费队列的任务是否过期.
    @Override
    public void run() {
        if (!canConsume) {
            return;
        }
        lock.lock();
        while (true) {
            RegisterObject registerObject = producerConsumeQueue.peek();
            if (System.currentTimeMillis() > registerObject.getExpireTime()) {
                try {
                    this.client.delete().forPath(
                        NativeProperties.getProducerConsumePath() + "/" + registerObject.getNode());
                    int limit = queryTypeLimiterMap.get(registerObject.getQueryType());
                    if (limit >= 0) {
                        queryTypeLimiterMap.put(registerObject.getQueryType(), ++limit);
                    }
                    producerConsumeQueue.poll();
                } catch (Exception e) {
                    LOGGER.error(String.format(
                        "presto limiter timing job delete expire node fail! the error is [%s]",
                        e.getMessage()));
                    break;
                }

            } else {
                break;
            }
        }
        moveNodesToConsumePath();
        lock.unlock();
    }

    public void moveNodesToConsumePath() {
        List<RegisterObject> unRunableObjectList = new ArrayList<>(); //由于类型limit限制无法执行的对象.
        while (canConsume && producerConsumeQueue.size() < NativeProperties.getPrestoLimitCnt()
            && producerRegisterQueue.size() > 0) {
            RegisterObject registerObject = producerRegisterQueue.peek();
            if (System.currentTimeMillis() > registerObject.getExpireTime()) {
                deleteRegisterNode(registerObject);
                producerRegisterQueue.poll();
            } else {
                if (registerObject.getPriority() >= NativeProperties
                    .getPrestoLimitHighPriorityMinScore()
                    || producerConsumeQueue.size()
                    < NativeProperties.getPrestoLimitCnt() - NativeProperties
                    .getPrestoLimitHighPriorityReserved()) {
                    int limit = queryTypeLimiterMap.get(registerObject.getQueryType());
                    if (limit != 0) {
                        if (moveTopNode()) {
                            if (limit > 0) {
                                queryTypeLimiterMap.put(registerObject.getQueryType(), --limit);
                            }
                        } else {
                            break;
                        }
                    } else {
                        producerRegisterQueue.poll();
                        unRunableObjectList.add(registerObject);
                    }
                } else {
                    break;
                }
            }
        }
        if (unRunableObjectList.size() > 0) {
            producerRegisterQueue.addAll(unRunableObjectList);
        }
    }

    public void deleteConsumeNodeFromQueue(RegisterObject registerObject) {
        lock.lock();
        if (this.producerConsumeQueue.contains(registerObject)) {
            this.producerConsumeQueue.remove(registerObject);
            int limit = queryTypeLimiterMap.get(registerObject.getQueryType());
            if (limit >= 0) {
                queryTypeLimiterMap.put(registerObject.getQueryType(), ++limit);
            }
        }
        moveNodesToConsumePath();
        lock.unlock();
    }

    public void addConsumeNodeToQueue(RegisterObject registerObject) {
        lock.lock();
        if (!this.producerConsumeQueue.contains(registerObject)) {
            int limit = queryTypeLimiterMap.get(registerObject.getQueryType());
            if (limit > 0) {
                this.producerConsumeQueue.add(registerObject);
                queryTypeLimiterMap.put(registerObject.getQueryType(), --limit);
            } else if (limit == 0) {
                deleteConsumeNode(registerObject);
                LOGGER.error(
                    "unexpected error, the type limiter is zero, but add node to consume path also");
            } else {
                this.producerConsumeQueue.add(registerObject);
            }
        }
        lock.unlock();
    }

    public void addRegisterNodeToQueue(RegisterObject registerObject) {
        lock.lock();
        if (!this.producerRegisterQueue.contains(registerObject)) {
            this.producerRegisterQueue.add(registerObject);
        }
        moveNodesToConsumePath();
        lock.unlock();
    }

    public void deleteRegisterNodeFromQueue(RegisterObject registerObject) {
        lock.lock();
        if (this.producerRegisterQueue.contains(registerObject)) {
            this.producerRegisterQueue.remove(registerObject);
        }
        lock.unlock();
    }

    public void setCanConsume(boolean canConsume) {
        this.canConsume = canConsume;
    }

    private Map<QueryType, Integer> initQueryTypeLimitMap() {
        Map<QueryType, Integer> map = new HashMap<>();
        for (QueryType queryType : QueryType.values()) {
            map.put(queryType, queryType.getLimit());
        }
        return map;
    }

    private void deleteRegisterNode(RegisterObject registerObject) {
        try {
            this.client.delete().forPath(
                NativeProperties.getProducerRegisterPath() + "/" + registerObject.getNode());
        } catch (Exception e) {
            LOGGER.error(String.format(
                "presto limiter delete producer register path expire node fail! the error is [%s]",
                e.getMessage()));
        }
    }

    private void deleteConsumeNode(RegisterObject registerObject) {
        try {
            this.client.delete().forPath(
                NativeProperties.getProducerConsumePath() + "/" + registerObject.getNode());
        } catch (Exception e) {
            LOGGER.error(String.format(
                "presto limiter delete producer consume path expire node fail! the error is [%s]",
                e.getMessage()));
        }
    }

    //如果失败，等待下一次.
    private boolean moveTopNode() {
        try {
            RegisterObject registerObject = new RegisterObject(producerRegisterQueue.peek());
            registerObject.setExpireTime(
                System.currentTimeMillis() + NativeProperties.getProducerConsumeExpireMills());
            if (canConsume) {
                this.client.inTransaction().delete().forPath(
                    NativeProperties.getProducerRegisterPath() + "/" + registerObject.getNode())
                    .and()
                    .create()
                    .forPath(
                        NativeProperties.getProducerConsumePath() + "/" + registerObject.getNode(),
                        JSON.toJSONBytes(registerObject)).and().commit();
            } else {
                return false;
            }
            this.producerRegisterQueue.poll();
            this.producerConsumeQueue.add(registerObject);
            return true;
        } catch (Exception e) {
            LOGGER.error("presto limiter move node from register path to consume path fail", e);
            return false;
        }
    }
}
