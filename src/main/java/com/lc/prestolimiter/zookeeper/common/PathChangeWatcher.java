package com.lc.prestolimiter.zookeeper.common;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.PathUtils;

public class PathChangeWatcher implements PathChildrenCacheListener {

    private final CuratorFramework curator;
    private final String path;
    private PathChangeListener pathChangeListener;

    private PathChildrenCache cache;

    /**
     * constructor.
     */
    public PathChangeWatcher(CuratorFramework curator,
        String path, PathChangeListener pathChangeListener) {
        this.curator = curator;
        PathUtils.validatePath(path);
        this.path = path;
        this.pathChangeListener = pathChangeListener;
    }

    /**
     * start path children cache.
     */
    public void start(StartMode startMode) throws Exception {

        assert path != null && !path.trim().isEmpty();

        cache = new PathChildrenCache(curator, path, false);

        cache.getListenable().addListener(this);

        cache.start(startMode);
    }

    /**
     * start with normal.
     */
    public void start() throws Exception {
        this.start();
    }

    /**
     * close cache quietly.
     */
    public void stop() {
        if (cache != null) {
            CloseableUtils.closeQuietly(cache);
        }
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
        pathChangeListener.childEvent(PathChildrenEvent.createPathChildrenEvent(event));
    }
}
