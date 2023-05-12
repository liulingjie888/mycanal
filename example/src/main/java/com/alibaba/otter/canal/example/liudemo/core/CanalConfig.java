package com.alibaba.otter.canal.example.liudemo.core;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author liulingjie
 * @date 2022/12/21 15:18
 */
@ConfigurationProperties(prefix = "canal.client")
@Component
@Data
public class CanalConfig {

    /**
     * cluster集群模式下的zookeeper地址（暂时无用）
     */
    private String zkServers;

    /**
     * 单节点时的地址端口ip:port
     */
    private String address;

    /**
     * canal实例
     */
    private String destination;

    /**
     * 密码
     */
    private String username;

    /**
     * canal账号
     */
    private String password;

    /**
     * 一次获取变更记录最大数
     */
    private int batchSize = 1024;

    /**
     * 同canal filter，用于过滤database或者table的相关数据。
     */
    private String filter = "";

    /**
     * 开启debug，会把每条消息的详情打印
     */
    private boolean debug = false;


    /** 异常处理策略
     * 1:retry，重试，重试默认为3次，由retryTimes参数决定，如果重试次数达到阈值，则跳过，并且记录日志。
     * 2:ignore,直接忽略，不重试，记录日志。
     */
    private int exceptionStrategy = 1;

    /**
     * 重试次数，异常处理是retry有效
     */
    private int retryTimes = 3;

    /**
     * 当binlog没有数据时，主线程等待的时间，单位ms,大于0
     */
    private int waitingTime = 1000;

    /**
     * 是否开启
     */
    private boolean open = false;

    private int executeRate = 1000;

}
