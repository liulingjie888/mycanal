package com.alibaba.otter.canal.example.demo;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

/**
 * @author liulingjie
 * @date 2022/12/21 15:00
 */
public interface ISqlEventExecutor {

    /**
     * 强烈建议捕获异常
     * @param header
     * @param afterColumns
     */
    default void insert(CanalEntry.Header header, List<CanalEntry.Column> afterColumns) {}

    /**
     * 强烈建议捕获异常
     * @param header
     * @param beforeColumns 变化之前的列数据
     * @param afterColumns 变化之后的列数据
     */
    default void update(CanalEntry.Header header,List<CanalEntry.Column> beforeColumns,List<CanalEntry.Column> afterColumns) {}

    /**
     * 强烈建议捕获异常
     * @param header
     * @param beforeColumns 删除之前的列数据
     */
    default void delete(CanalEntry.Header header,List<CanalEntry.Column> beforeColumns) {}

    /**
     * 创建表
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    default void createTable(CanalEntry.Header header,String sql) {}

    /**
     * 修改表结构,即alter指令，需要声明：通过alter增加索引、删除索引，也是此操作。
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    default void alterTable(CanalEntry.Header header,String sql) {}

    /**
     * 清空、重建表
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    default void truncateTable(CanalEntry.Header header,String sql) {}

    /**
     * 重命名schema或者table，注意
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    default void rename(CanalEntry.Header header,String sql) {}
    /**
     * 创建索引,通过“create index on table”指令
     * @param header 可以从header中获得schema、table的名称
     * @param sql
     */
    default void createIndex(CanalEntry.Header header,String sql) {}

    /**
     * 删除索引，通过“delete index on table”指令
     * @param header      * 可以从header中获得schema、table的名称
     * @param sql
     */
    default void deleteIndex(CanalEntry.Header header,String sql) {}

    /**
     * 强烈建议捕获异常，非上述已列出的其他操作，非核心
     * 除了“insert”、“update”、“delete”操作之外的，其他类型的操作.
     * 默认实现为“无操作”
     * @param entry
     */
    default void whenOthers(CanalEntry.Entry entry) {}
}
