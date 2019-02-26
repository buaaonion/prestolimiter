package com.lc.prestolimiter.service;

import com.lc.prestolimiter.Producer.ProducerService;
import com.lc.prestolimiter.common.NativeProperties;
import com.lc.prestolimiter.common.QueryType;
import com.lc.prestolimiter.common.Switchable;
import com.lc.prestolimiter.consumer.RegisterService;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;

/**
 * presto limiter服务.该服务的主要功能是通过将所有端访问presto的请求统一管理起来，来限制对presto集群的并发访问量。
 * 该服务主要包含两个模块：生产者模块与消费者模块。
 * 消费者模块：
 * 1）只可能有一台机器成为消费者，其他的机器作为热备，一旦作为消费者的机器出现问题，其他机器通过竞争分布式锁来让自己成为消费者。
 * 2）消费者机器主要功能就是为所有端查询请求统一分配资源，将生产者在注册路径下的注册的节点，挪到可执行路径下去。
 * 生产者模块：
 * 1）服务调用者主要调用的模块，该模块通过用户提交的查询优先级以及查询类型，在生产者的注册路径下注册一个顺序节点，并监听可执行路径，
 * 一旦当前节点被挪到可执行路径，则获得查询资格，开始执行。
 * 两个模块都是永久节点，通过超时机制来防止资源获取者长久等待以及释放资源。
 * 查询优先级区间：【1-100】
 * 消费者为高于一定优先级的请求预留了一定的资源，低优先级的请求无法使用所有的资源，保证了线上高优先级的任务能够及时执行。
 * 使用服务时先初始化PrestoLimiterService实例，并调用{@code start}，然后在查询的过程中调用{@code getExecutePermition}方法，
 * 一但获取执行权限成功，无论后面的查询是否成功，都要调用{@code deleteExecutePermition}方法释放资源。
 */
public class PrestoLimiterService implements Switchable {

    private final CuratorFramework client;
    private final ProducerService producerService;
    private final RegisterService consumerRegisterService;
    private final boolean consumable;

    /**
     * 创建一个presto limiter服务，并设置当前进程能否作为消费者.
     *
     * @param consumable 是否可作为消费者，一般使用线上的机器作为消费者，测试以及其他的机器由于不稳定，不要设置为消费者.
     */
    public PrestoLimiterService(boolean consumable) throws UnknownHostException {
        client = CuratorFrameworkFactory.newClient(
            NativeProperties.getZkAddr(),
            NativeProperties.getZkSessionTimeoutMs(),
            NativeProperties.getZkCnxnTimeoutMs(),
            new RetryNTimes(NativeProperties.getZkRetryTimes(),
                NativeProperties.getZkSleepMsBetweenRetries()));
        this.producerService = new ProducerService(client);
        this.consumerRegisterService = new RegisterService(client);
        this.consumable = consumable;
    }

    /**
     * start the service.
     */
    public boolean start() {
        synchronized (client) {
            if (client.getState() == CuratorFrameworkState.LATENT) {
                client.start();
            }
        }
        return this.producerService.start()
            && (consumable ? this.consumerRegisterService.start() : true);
    }

    /**
     * stop the service.
     */
    public boolean stop() {
        this.consumerRegisterService.stop();
        this.producerService.stop();
        synchronized (client) {
            if (client.getState() == CuratorFrameworkState.STARTED) {
                client.close();
            }
        }
        return true;
    }

    /**
     * 或者执行的优先级，在获取到资源之前或者是超时执行，现成会被挂起.
     *
     * @param priority 查询的优先级.
     * @param queryType 查询的类型.
     * @return zk上可执行节点的名称，如发生异常，返回null.
     */
    public String getExecutePermission(int priority, QueryType queryType) {
        return getExecutePermissionWithTimeoutMills(priority, queryType,
            NativeProperties.getProducerRegisterDefaultExpireMills());
    }

    /**
     * get execute permission with wait time out.
     *
     * @param priority 查询的优先级.
     * @param queryType 查询的类型.
     * @param waitTimeoutMills 等待超时, 默认1小时, 最大6小时.
     * @return zk上可执行节点的名称，如发生异常，返回null.
     */
    public String getExecutePermissionWithTimeoutMills(int priority, QueryType queryType,
        long waitTimeoutMills) {
        if (priority <= 0 || queryType == null || waitTimeoutMills <= 0) {
            return null;
        }
        return producerService.getExecutePermission(priority, queryType, waitTimeoutMills);
    }

    /**
     * 从zk可执行路径上删除当前节点.
     *
     * @param path zk上可执行节点的名称.
     */
    public void deleteExecutePermition(String path) {
        if (!StringUtils.isEmpty(path)) {
            producerService.deleteExecutePermission(path);
        }
    }
}
