package com.lc.prestolimiter.zookeeper.common;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.utils.PathUtils;

public class PathChildrenEvent {

    private String path;

    private String node;

    private Type eventType;

    private byte[] data;

    private PathChildrenEvent(String path, String node,
        Type eventType, byte[] data) {
        this.path = path;
        this.node = node;
        this.eventType = eventType;
        this.data = data;
    }

    private PathChildrenEvent(Type eventType) {
        this.eventType = eventType;
    }

    /**
     * create event for children node change.
     */
    public static PathChildrenEvent createPathChildrenEvent(PathChildrenCacheEvent event) {
        if (event.getType().equals(Type.CHILD_ADDED)
            || event.getType().equals(Type.CHILD_REMOVED)
            || event.getType().equals(Type.CHILD_UPDATED)) {
            String path = event.getData().getPath();
            byte[] data = event.getData().getData();
            PathUtils.validatePath(path);
            int i = path.lastIndexOf('/');
            if ((i + 1) >= path.length()) {
                return new PathChildrenEvent("/", "", event.getType(), data);
            }
            String node = path.substring(i + 1);
            String parentPath = (i > 0) ? path.substring(0, i) : "/";
            return new PathChildrenEvent(parentPath, node, event.getType(), data);
        } else {
            return new PathChildrenEvent(event.getType());
        }
    }

    public String getPath() {
        return path;
    }

    public String getNode() {
        return node;
    }

    public Type getEventType() {
        return eventType;
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public String toString() {
        return "PathChildrenEvent{"
            + "path='" + path + '\''
            + ", node='" + node + '\''
            + ", evenType=" + eventType.name() + '}';
    }
}
