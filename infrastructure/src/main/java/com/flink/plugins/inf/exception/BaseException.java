package com.flink.plugins.inf.exception;

import lombok.ToString;

/**
 * @fileName: BaseException.java
 * @description: 基础异常类
 * @author: huangshimin
 * @date: 2022/9/30 4:12 PM
 */
@ToString
public abstract class BaseException extends RuntimeException {
    private ExceptionEnums exceptionEnums;

    public BaseException(ExceptionEnums exceptionEnums) {
        this.exceptionEnums = exceptionEnums;
    }
}
