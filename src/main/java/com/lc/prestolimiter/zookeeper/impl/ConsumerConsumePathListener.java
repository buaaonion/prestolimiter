package com.lc.prestolimiter.zookeeper.impl;

import com.alibaba.fastjson.JSON;
import com.lc.prestolimiter.common.RegisterObject;
import com.lc.prestolimiter.consumer.ConsumeService;
import com.lc.prestolimiter.zookeeper.common.PathChangeListener;
import com.lc.prestolimiter.zookeeper.common.PathChildrenEvent;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;

public class ConsumerConsumePathListener implements PathChangeListener {

    private ConsumeService consumeService;
    private AtomicInteger initialized;

    public ConsumerConsumePathListener(ConsumeService consumeService,
        AtomicInteger initialized) {
        this.consumeService = consumeService;
        this.initialized = initialized;
    }

    @Override
    public synchronized void childEvent(PathChildrenEvent pathChildrenEvent) {
        if (pathChildrenEvent.getEventType() == Type.CHILD_REMOVED) {
            RegisterObject registerObject = JSON
                .parseObject(pathChildrenEvent.getData(), RegisterObject.class);
            consumeService.deleteConsumeNodeFromQueue(registerObject);
        } else if (pathChildrenEvent.getEventType() == Type.CHILD_ADDED) {
            RegisterObject registerObject = JSON
                .parseObject(pathChildrenEvent.getData(), RegisterObject.class);
            consumeService.addConsumeNodeToQueue(registerObject);
        } else if (pathChildrenEvent.getEventType() == Type.INITIALIZED) {
            if (this.initialized.decrementAndGet() == 0) {
                consumeService.setCanConsume(true);
            }
        }
    }
}
