package com.lc.prestolimiter.zookeeper.impl;

import com.lc.prestolimiter.common.NativeProperties;
import com.lc.prestolimiter.consumer.RegisterService;
import com.lc.prestolimiter.zookeeper.common.PathChangeListener;
import com.lc.prestolimiter.zookeeper.common.PathChildrenEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerLockPathListener implements PathChangeListener {

    private static final Logger LOGGER = LoggerFactory
        .getLogger(ConsumerLockPathListener.class.getName());

    private RegisterService registerService;

    public ConsumerLockPathListener(RegisterService registerService) {
        this.registerService = registerService;
    }

    @Override
    public synchronized void childEvent(PathChildrenEvent childrenEvent) {
        switch (childrenEvent.getEventType()) {
            case CHILD_REMOVED:
                if (childrenEvent.getNode().equals(NativeProperties.getConsumerLockNode())) {
                    registerService.removeConsumerIdentity();
                    registerService.lockThenConsume(false);
                }
                break;
            case CONNECTION_SUSPENDED:
                registerService.closeConsumeState();
                break;
            case CONNECTION_RECONNECTED:
                if (registerService.checkSelfIsTheConsume()) {
                    registerService.openConsumeState();
                }
                break;
            case CONNECTION_LOST:
                registerService.removeConsumerIdentity();
                break;
            default:
                break;
        }
    }
}
