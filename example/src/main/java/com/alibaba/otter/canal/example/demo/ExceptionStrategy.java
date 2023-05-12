package com.alibaba.otter.canal.example.demo;

import lombok.Data;

public enum ExceptionStrategy {

    /**
     * 错误重试
     */
    RETRY(1),

    /**
     * 错误忽略
     */
    IGNORE(2);

    int code;

    ExceptionStrategy(int code) {
        this.code = code;
    }

    public static ExceptionStrategy codeOf(Integer code) {
        if(code == null) {
            return null;
        }
        for(ExceptionStrategy e : ExceptionStrategy.values()) {
            if(e.code == code) {
                return e;
            }
        }
        return null;
    }
}