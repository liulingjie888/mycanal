package com.alibaba.otter.canal.example.liudemo.core;

import lombok.Data;

/**
 * @author liulingjie
 * @date 2022/12/22 11:29
 */
@Data
public class SysCacheDTO {

    private Long id;

    private String cacheKey;

    private String reTable;

    private String params;

    private String filter;
}
