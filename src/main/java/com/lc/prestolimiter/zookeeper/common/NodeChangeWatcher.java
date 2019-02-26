package com.lc.prestolimiter.zookeeper.common;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.PathUtils;

public class NodeChangeWatcher {

    private final CuratorFramework curator;
    private final String nodePath;
    private NodeChangeListener nodeChangeListener;

    private NodeCache cache;

    /**
     * constructor.
     */
    public NodeChangeWatcher(CuratorFramework curator,
        String nodePath, NodeChangeListener nodeChangeListener) {
        this.curator = curator;
        PathUtils.validatePath(nodePath);
        this.nodePath = nodePath;
        this.nodeChangeListener = nodeChangeListener;
    }

    /**
     * start listener and add callback.
     */
    public void start() throws Exception {
        assert nodePath != null && nodePath.isEmpty();

        cache = new NodeCache(curator, nodePath);

        cache.getListenable().addListener(() -> {
            byte[] bytes = cache.getCurrentData().getData();
            nodeChangeListener.nodeChange(byte2String(bytes));
        });
        cache.start();
    }

    private String byte2String(byte[] bytes) {
        if (bytes != null) {
            String data = new String(bytes);
            if (data != null && !data.trim().isEmpty()) {
                return data;
            }
        }
        return null;
    }

    /**
     * close watcher cache quietly.
     */
    public void stop() {
        if (cache != null) {
            CloseableUtils.closeQuietly(cache);
        }
    }
}
