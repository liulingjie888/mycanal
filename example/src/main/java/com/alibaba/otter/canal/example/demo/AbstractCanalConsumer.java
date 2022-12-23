package com.alibaba.otter.canal.example.demo;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import lombok.Setter;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.CollectionUtils;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


/**
 * @author liulingjie
 * @date 2022/12/20 15:46
 */

/**
 * Description
 * <p>
 * </p>
 * DATE 17/10/19.
 *
 * @author liuguanqing.
 */
public abstract class AbstractCanalConsumer {

    /**
     * 换行符
     */
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;

    static {
        // batch的摘要信息打印模板初始化
        StringBuilder sb = new StringBuilder();
        sb.append(SEP)
                .append("-------------Batch-------------")
                .append(SEP)
                .append("* Batch Id: [{}] ,count : [{}] , Mem size : [{}] , Time : {}")
                .append(SEP)
                .append("* Start : [{}] ")
                .append(SEP)
                .append("* End : [{}] ")
                .append(SEP)
                .append("-------------------------------")
                .append(SEP);
        contextFormat = sb.toString();
        // 更数据摘要信息打印模板初始化
        sb = new StringBuilder();
        sb.append(SEP)
                .append("+++++++++++++Row+++++++++++++>>>")
                .append("binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms")
                .append(SEP);
        rowFormat = sb.toString();
        // 事务开始结束摘要信息打印模板初始化
        sb = new StringBuilder();
        sb.append(SEP)
                .append("===========Transaction {} : {}=======>>>")
                .append("binlog[{}:{}] , executeTime : {} , delay : {}ms")
                .append(SEP);
        transactionFormat = sb.toString();
    }

    /**
     * 日志
     */
    private static final Logger log = LoggerFactory.getLogger(AbstractCanalConsumer.class);

    /**
     * 线程异常中断后处理
     */
    protected Thread.UncaughtExceptionHandler handler = (t, ex) -> log.error("parse events has an error", ex);

    /**
     * batch的摘要信息打印模板
     */
    protected static String contextFormat = null;

    /**
     * 变更数据摘要信息打印模板
     */
    protected static String rowFormat = null;

    /**
     * 事务开始结束摘要信息打印模板
     */
    protected static String transactionFormat = null;

    /**
     * 时间模板
     */
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 配置
     */
    @Setter
    private CanalConfig canalConfig;

    /**
     * 是否运行
     */
    private volatile boolean running = false;

    /**
     * 消费变化数据线程
     */
    protected Thread thread;

    /**
     * 连接
     */
    private CanalConnector connector;

    /**
     * 回调方法执行者
     */
    @Setter
    List<ISqlEventExecutor> executorList = new ArrayList<>();

    public void addExector(ISqlEventExecutor executor) {
        executorList.add(executor);
    }

    /**
     * 启动
     */
    public synchronized void start() {

        if(running) {
            return;
        }

        String[] segments = canalConfig.getAddress().split(":");
        SocketAddress socketAddress = new InetSocketAddress(segments[0],Integer.parseInt(segments[1]));
        connector = CanalConnectors.newSingleConnector(socketAddress,canalConfig.getDestination(), canalConfig.getUsername(), canalConfig.getPassword());

        thread = new Thread(this::process);
        thread.setUncaughtExceptionHandler(handler);
        thread.start();

        running = true;
    }

    /**
     * 关闭
     */
    protected synchronized void stop() {
        if (!running) {
            return;
        }
        // 停止
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        MDC.remove("destination");
    }

    /**
     *
     * 用于控制当连接异常时，重试的策略，我们不应该每次都是立即重试，否则将可能导致大量的错误，在空转时导致CPU过高的问题
     * sleep策略基于简单的累加，最长不超过3S
     */
    private void sleepWhenFailed(int times) {
        if(times <= 0) {
            return;
        }
        try {
            int sleepTime = 1000 + times * 100;//最大sleep 3s。
            Thread.sleep(sleepTime);
        } catch (Exception ex) {
            //
        }
    }

    /**
     * 执行接受entry
     */
    protected void process() {
        int times = 0;
        // 报错循环重试
        while (running) {
            try {

                // 报错延时重试
                sleepWhenFailed(times);

                if(!running) {
                    break;
                }

                MDC.put("destination", canalConfig.getDestination());

                // 连接及订阅
                connector.connect();
                connector.subscribe(canalConfig.getFilter());

                times = 0;

                // 循环监听消费数据
                while (running) {
                    Message message = connector.getWithoutAck(canalConfig.getBatchSize()); // 获取指定数量的数据，不确认
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    // 没有数据变更
                    if (batchId == -1 || size == 0) {
                        try {
                            Thread.sleep(canalConfig.getWaitingTime());
                        } catch (InterruptedException e) {
                            //
                        }
                        continue;
                    }

                    // 打印
                    if (canalConfig.isDebug()) {
                        printBatch(message, batchId);
                    }

                    // 遍历处理每条变更数据
                    for(CanalEntry.Entry entry : message.getEntries()) {
                        session(entry);
                    }

                    // ack all the time。
                    connector.ack(batchId);
                }
            } catch (Exception ex) {
                log.error("process error!", ex);
                if(times > 20) {
                    times = 0;
                }
                times++;
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    /**
     * 处理entry
     * @param entry
     */
    protected void session(CanalEntry.Entry entry) {
        /**
         * 获取类型：
         */
        CanalEntry.EntryType entryType = entry.getEntryType();
        int errorTimes = 0;
        boolean success = false;

        label:
        while (!success) {

            // 错误重试
            if(errorTimes > 0) {
                /**
                 * 1:retry，重试，重试默认为3次，由retryTimes参数决定，如果重试次数达到阈值，则跳过，并且记录日志。
                 * 2:ignore,直接忽略，不重试，记录日志。
                 */
                switch (ExceptionStrategy.codeOf(canalConfig.getExceptionStrategy())) {
                    case RETRY:
                        if(errorTimes >= canalConfig.getRetryTimes()) {
                            break label;
                        }
                        break;
                    case IGNORE:
                    default:
                        break label;
                }
            }

            try {
                switch (entryType) {
                    case TRANSACTIONBEGIN:
                        transactionBegin(entry);
                        break;
                    case TRANSACTIONEND:
                        transactionEnd(entry);
                        break;
                    case ROWDATA:
                        rowData(entry);
                        break;
                    default:
                        break;
                }
                success = true;
            } catch (Exception ex) {
                errorTimes++;
                log.error("session parse event has an error ,times: + " + errorTimes + ", data:" + entry, ex);
            }

        }

        if(canalConfig.isDebug() && success) {
            log.info("session parse event success,position:" + entry.getHeader().getLogfileOffset());
        }
    }

    /**
     * 处理单条DML记录
     * @param entry
     * @throws Exception
     */
    private void rowData(CanalEntry.Entry entry) throws Exception {
        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        CanalEntry.EventType eventType = rowChange.getEventType();
        CanalEntry.Header header = entry.getHeader();
        long executeTime = header.getExecuteTime();
        long delayTime = new Date().getTime() - executeTime;
        String sql = rowChange.getSql();
        if(canalConfig.isDebug()) {
            if (eventType == CanalEntry.EventType.QUERY || rowChange.getIsDdl()) {
                log.info("------SQL----->>> type : {} , sql : {} ", eventType.getNumber(), sql);
            }
            log.info(rowFormat,
                    header.getLogfileName(),
                    header.getLogfileOffset(),
                    header.getSchemaName(),
                    header.getTableName(),
                    eventType,
                    executeTime,
                    delayTime);
        }

        try {
            // DDL
            switch (eventType) {
                case CREATE:
                    executorList.parallelStream().forEach(executor -> executor.createTable(header,sql));
                    return;
                case ALTER:
                    executorList.parallelStream().forEach(executor -> executor.alterTable(header,sql));
                    return;
                case TRUNCATE:
                    executorList.parallelStream().forEach(executor -> executor.truncateTable(header,sql));
                    return;
                case RENAME:
                    executorList.parallelStream().forEach(executor -> executor.rename(header,sql));
                    return;
                case CINDEX:
                    executorList.parallelStream().forEach(executor -> executor.createIndex(header,sql));
                    return;
                case DINDEX:
                    executorList.parallelStream().forEach(executor -> executor.deleteIndex(header,sql));
                    return;
                //case ERASE:
                //    log.debug("parse event : erase,ignored!");
                //    return;
                //case QUERY:
                //    log.debug("parse event : query,ignored!");
                //    return;
                default:
                    break;
            }

            // DML
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                switch (eventType) {
                    case DELETE:
                        executorList.parallelStream().forEach(executor -> executor.delete(header, rowData.getBeforeColumnsList()));
                        break;
                    case INSERT:
                        executorList.parallelStream().forEach(executor -> executor.insert(header, rowData.getBeforeColumnsList()));
                        break;
                    case UPDATE:
                        executorList.parallelStream().forEach(executor -> executor.update(header, rowData.getBeforeColumnsList(), rowData.getAfterColumnsList()));
                        break;
                    default:
                        executorList.parallelStream().forEach(executor -> executor.whenOthers(entry));
                }
            }
        } catch (Exception ex) {
            log.error("rowData process event error ,",ex);
            log.error(rowFormat,
                    header.getLogfileName(),
                    header.getLogfileOffset(),
                    header.getSchemaName(),
                    header.getTableName(),
                    eventType,
                    executeTime,
                    delayTime);
            throw ex;
        }
    }

    /**
     * 事务开始打印
     * @param entry
     */
    public void transactionBegin(CanalEntry.Entry entry) {
        if(!canalConfig.isDebug()) {
            return;
        }
        try {
            CanalEntry.TransactionBegin begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
            // 打印事务头信息，执行的线程id，事务耗时
            CanalEntry.Header header = entry.getHeader();
            long executeTime = header.getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
            log.info(transactionFormat,
                    "begin",
                    begin.getTransactionId(),
                    header.getLogfileName(),
                    header.getLogfileOffset(),
                    header.getExecuteTime(),
                    delayTime);
        } catch (Exception e) {
            log.error("parse event has an error , data:" + entry.toString(), e);
        }
    }

    /**
     * 事务结束打印
     * @param entry
     */
    public void transactionEnd(CanalEntry.Entry entry) {
        if(!canalConfig.isDebug()) {
            return;
        }
        try {
            CanalEntry.TransactionEnd end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
            // 打印事务提交信息，事务id
            CanalEntry.Header header = entry.getHeader();
            long executeTime = header.getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;
            log.info(transactionFormat,
                    "end",
                    end.getTransactionId(),
                    header.getLogfileName(),
                    header.getLogfileOffset(),
                    header.getExecuteTime(),
                    delayTime);
        } catch (Exception e) {
            log.error("parse event has an error , data:" + entry.toString(), e);
        }
    }


    /**
     * 答应当前batch的摘要信息
     * @param message
     * @param batchId
     */
    protected void printBatch(Message message, long batchId) {
        List<CanalEntry.Entry> entries = message.getEntries();
        if(CollectionUtils.isEmpty(entries)) {
            return;
        }

        long memSize = 0;
        for (CanalEntry.Entry entry : entries) {
            memSize += entry.getHeader().getEventLength();
        }
        int size = entries.size();
        String startPosition = buildPosition(entries.get(0));
        String endPosition = buildPosition(message.getEntries().get(size - 1));

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        log.info(contextFormat,
                batchId,
                size,
                memSize,
                format.format(new Date()),
                startPosition,
                endPosition);
    }

    /**
     *  打印单个entry信息
     * @param entry
     * @return
     */
    protected String buildPosition(CanalEntry.Entry entry) {
        CanalEntry.Header header = entry.getHeader();
        long time = header.getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        StringBuilder sb = new StringBuilder();
        sb.append(header.getLogfileName())
                .append(":")
                .append(header.getLogfileOffset())
                .append(":")
                .append(header.getExecuteTime())
                .append("(")
                .append(format.format(date))
                .append(")");
        return sb.toString();
    }


}