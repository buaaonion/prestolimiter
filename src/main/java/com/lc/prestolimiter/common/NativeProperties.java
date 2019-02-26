package com.lc.prestolimiter.common;

import com.lc.prestolimiter.Exception.PrestoLimiterException;
import java.io.FileInputStream;
import java.time.Duration;
import java.util.Properties;
import org.apache.curator.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

public class NativeProperties {

    private static final Logger LOGGER = LoggerFactory.getLogger(NativeProperties.class.getName());
    private static final String zkAddr;
    private static final int zkCnxnTimeoutMs;
    private static final int zkRetryTimes;
    private static final int zkSessionTimeoutMs;
    private static final int zkSleepMsBetweenRetries;
    private static final String consumerLockPath;
    private static final String consumerLockNode;
    private static final String producerRegisterPath;
    private static final String producerConsumePath;
    private static final long producerRegisterDefaultExpireMills;
    private static final long producerRegisterMaxExpireMills;
    private static final long producerConsumeExpireMills;
    private static final int prestoLimitCnt;
    private static final int prestoLimitHighPriorityReserved;
    private static final int prestoLimitHighPriorityMinScore;

    static {
        try {
            Properties limiterProperties = new Properties();
            limiterProperties.load(new FileInputStream("./limiter.properties"));
            zkAddr = limiterProperties.getProperty("zookeeper.addr");
            assert checkZkAddr(zkAddr) : "presto limiter zkAddr unValid";
            zkCnxnTimeoutMs = Integer
                .parseInt(limiterProperties.getProperty("zookeeper.cnxnTimeoutMs"));
            assert zkCnxnTimeoutMs > 0 : "presto limiter zkCnxnTimeoutMs <= 0";
            zkRetryTimes = Integer.parseInt(limiterProperties.getProperty("zookeeper.retryTimes"));
            assert zkRetryTimes > 0 : "presto limiter zkRetryTimes <= 0";
            zkSessionTimeoutMs = Integer
                .parseInt(limiterProperties.getProperty("zookeeper.sessionTimeoutMs"));
            assert zkSessionTimeoutMs > 0 : "presto limiter zkSessionTimeoutMs <= 0";
            zkSleepMsBetweenRetries = Integer
                .parseInt(limiterProperties.getProperty("zookeeper.sleepMsBetweenRetries"));
            assert zkSleepMsBetweenRetries > 0 : "presto limiter zkSleepMsBetweenRetries <= 0";
            consumerLockPath = limiterProperties.getProperty("consumer.lock.path");
            PathUtils.validatePath(consumerLockPath);
            consumerLockNode = limiterProperties.getProperty("consumer.lock.node");
            assert consumerLockNode != null && consumerLockNode.length() != 0;
            producerRegisterPath = limiterProperties.getProperty("producer.register.path");
            PathUtils.validatePath(producerRegisterPath);
            producerConsumePath = limiterProperties.getProperty("producer.consume.path");
            PathUtils.validatePath(producerConsumePath);
            producerRegisterDefaultExpireMills = Duration.ofMinutes(Integer
                .parseInt(limiterProperties.getProperty("producer.register.defaultExpireMinutes")))
                .toMillis();
            assert
                producerRegisterDefaultExpireMills
                    > 0 : "presto limiter producerRegister defaultExpireMinutes <= 0";
            producerRegisterMaxExpireMills = Duration.ofMinutes(Integer
                .parseInt(limiterProperties.getProperty("producer.register.maxExpireMinutes")))
                .toMillis();
            assert
                producerRegisterMaxExpireMills
                    > 0 : "presto limiter producerRegister maxExpireMinutes <= 0";
            producerConsumeExpireMills = Duration.ofMinutes(Integer
                .parseInt(limiterProperties.getProperty("producer.consume.expireMinutes")))
                .toMillis();
            assert
                producerConsumeExpireMills > 0 : "presto limiter producerConsumeExpireMills <= 0";
            prestoLimitCnt = Integer.parseInt(limiterProperties.getProperty("presto.limit.count"));
            assert prestoLimitCnt > 0 : "presto limiter prestoLimitCnt <= 0";
            prestoLimitHighPriorityReserved = Integer
                .parseInt(limiterProperties.getProperty("presto.limit.highPriority.reserved"));
            assert prestoLimitHighPriorityReserved
                >= 0 : "presto limiter prestoLimitHighPriorityReserved < 0";
            prestoLimitHighPriorityMinScore = Integer
                .parseInt(limiterProperties.getProperty("presto.limit.highPriority.minScore"));
            assert prestoLimitHighPriorityMinScore
                > 0 : "presto limiter prestoLimitHighPriorityMinScore <= 0";
        } catch (Exception e) {
            LOGGER.error("load presto limiter properties fail!", e);
            throw new PrestoLimiterException("load presto limiter properties fail!", e);
        }
    }

    public static String getZkAddr() {
        return zkAddr;
    }

    public static int getZkCnxnTimeoutMs() {
        return zkCnxnTimeoutMs;
    }

    public static int getZkRetryTimes() {
        return zkRetryTimes;
    }

    public static int getZkSessionTimeoutMs() {
        return zkSessionTimeoutMs;
    }

    public static int getZkSleepMsBetweenRetries() {
        return zkSleepMsBetweenRetries;
    }

    public static String getConsumerLockPath() {
        return consumerLockPath;
    }

    public static String getConsumerLockNode() {
        return consumerLockNode;
    }

    public static String getProducerRegisterPath() {
        return producerRegisterPath;
    }

    public static String getProducerConsumePath() {
        return producerConsumePath;
    }

    public static long getProducerRegisterDefaultExpireMills() {
        return producerRegisterDefaultExpireMills;
    }

    public static long getProducerRegisterMaxExpireMills() {
        return producerRegisterMaxExpireMills;
    }

    public static long getProducerConsumeExpireMills() {
        return producerConsumeExpireMills;
    }

    public static int getPrestoLimitCnt() {
        return prestoLimitCnt;
    }

    public static int getPrestoLimitHighPriorityReserved() {
        return prestoLimitHighPriorityReserved;
    }

    public static int getPrestoLimitHighPriorityMinScore() {
        return prestoLimitHighPriorityMinScore;
    }

    private static boolean checkZkAddr(String zkAddr) {
        if (zkAddr != null && zkAddr.length() != 0) {
            String[] ips = zkAddr.split(",");
            for (String ip : ips) {
                if (!IPAddressUtil.isIPv4LiteralAddress(ip)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
