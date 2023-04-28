package com.flink.plugins.inf.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

/**
 * @fileName: ExceptionEnums.java
 * @description: 异常枚举
 * @author: huangshimin
 * @date: 2022/9/30 3:44 PM
 */
@AllArgsConstructor
@ToString
public enum ExceptionEnums {
    CONFIG_NOT_SUPPORT(20000, "非法配置，不支持!"),
    CONFIG_ERROR_SUFFIX(20001, "异常配置后缀!");

    @Getter
    private final int code;
    @Getter
    private final String msg;
}
