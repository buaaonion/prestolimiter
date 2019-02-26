package com.lc.prestolimiter.consumer;

import com.lc.prestolimiter.common.NativeProperties;
import com.lc.prestolimiter.common.Switchable;
import com.lc.prestolimiter.zookeeper.common.PathChangeWatcher;
import com.lc.prestolimiter.zookeeper.impl.ConsumerLockPathListener;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterService implements Switchable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterService.class.getName());

    private final String consumerLockData;
    private final CuratorFramework client;
    private ConsumeService consumeService;
    private final String consumerLockRealPath;
    private PathChangeWatcher lockPathChangeWatcher;
    private boolean isConsumer;

    public RegisterService(CuratorFramework client) throws UnknownHostException {
        this.client = client;
        this.consumerLockData = InetAddress.getLocalHost().getHostAddress();
        this.consumerLockRealPath =
            NativeProperties.getConsumerLockPath() + "/" + NativeProperties.getConsumerLockNode();
    }

    public boolean lockThenConsume(boolean addListening) {
        if (tryLock()) {
            this.consumeService = new ConsumeService(client); //每次开始消费都重新构建该对象.
            if (consumeService.start()) {
                LOGGER.error("presto limiter consumer!");
                this.isConsumer = true;
            } else {
                this.isConsumer = false;
                consumeService.stop();
                while (true) {
                    try {
                        this.client.delete().forPath(consumerLockRealPath);
                        break;
                    } catch (Exception e) {
                        LOGGER.error(String.format(
                            "get the presto limiter consumer lock, but the consume service start fail! "
                                + "then delete the lock fail! error info [%s]",
                            e.getMessage()));
                    }
                }
            }
        }
        if (addListening) {
            this.lockPathChangeWatcher = new PathChangeWatcher(client,
                NativeProperties.getConsumerLockPath(),
                new ConsumerLockPathListener(this));
            try {
                this.lockPathChangeWatcher.start();
            } catch (Exception e) {
                LOGGER.error("presto limiter listen for the consumer lock path error!", e);
                return false;
            }
        }
        return true;
    }

    private boolean tryLock() {
        try {
            this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                .forPath(consumerLockRealPath);
            return true;
        } catch (Exception e) {
            try {
                if (this.client.checkExists().forPath(consumerLockRealPath) != null) {
                    return false;
                } else {
                    LOGGER.error(String
                            .format("presto limiter consumer try lock error, the error info is [%s]"),
                        e.getMessage());
                    return tryLock();
                }
            } catch (Exception e1) {
                LOGGER.error(
                    String.format("presto limiter consumer try lock error, the error info is [%s]"),
                    e1.getMessage());
                return tryLock();
            }
        }
    }

    /**
     * 如果本机是消费者，则暂时关闭消费.
     */
    public void closeConsumeState() {
        if (consumeService != null) {
            consumeService.setCanConsume(false);
        }
    }

    /**
     * 如果本机是消费者，恢复临时关闭的消费状态.
     */
    public void openConsumeState() {
        if (this.isConsumer && consumeService != null) {
            consumeService.setCanConsume(true);
        }
    }

    /**
     * 移除消费者身份.
     */
    public void removeConsumerIdentity() {
        if (consumeService != null) {
            consumeService.stop();
        }
        this.isConsumer = false;
    }

    /**
     * 判断本机是否是消费者.
     */
    public boolean checkSelfIsTheConsume() {
        try {
            byte[] data = this.client.getData().forPath(this.consumerLockRealPath);
            if (data == null || data.length == 0) {
                return false;
            }
            String info = new String(data, Charset.forName("UTF-8"));
            if (consumerLockData.equals(info)) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            LOGGER
                .error(String.format("get lock data error, the error info is %s", e.getMessage()));
            return checkSelfIsTheConsume();
        }
    }

    @Override
    public boolean start() {
        return this.lockThenConsume(true);
    }

    @Override
    public boolean stop() {
        if (this.lockPathChangeWatcher != null) {
            this.lockPathChangeWatcher.stop();
        }
        if (this.consumeService != null) {
            this.consumeService.stop();
        }
        return true;
    }

}
