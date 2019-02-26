package com.lc.prestolimiter.zookeeper.impl;

import com.google.common.cache.Cache;
import com.lc.prestolimiter.common.RegisterObject;
import com.lc.prestolimiter.zookeeper.common.PathChangeListener;
import com.lc.prestolimiter.zookeeper.common.PathChildrenEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;

public class ProducerConsumePathListener implements PathChangeListener {

    private Cache<String, RegisterObject> waitExecuteQueryCache;

    public ProducerConsumePathListener(Cache<String, RegisterObject> waitExecuteQueryCache) {
        this.waitExecuteQueryCache = waitExecuteQueryCache;
    }

    @Override
    public synchronized void childEvent(PathChildrenEvent pathChildrenEvent) {
        if (pathChildrenEvent.getEventType() == Type.CHILD_ADDED) {
            String node = pathChildrenEvent.getNode();
            RegisterObject waitObject = waitExecuteQueryCache.getIfPresent(node);
            if (waitObject != null) {
                synchronized (waitObject) {
                    waitObject.notify();
                }
            }
        }
    }
}
