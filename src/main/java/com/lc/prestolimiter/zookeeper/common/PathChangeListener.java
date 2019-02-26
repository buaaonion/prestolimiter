package com.lc.prestolimiter.zookeeper.common;

public interface PathChangeListener {

    void childEvent(PathChildrenEvent pathChildrenEvent);
}
