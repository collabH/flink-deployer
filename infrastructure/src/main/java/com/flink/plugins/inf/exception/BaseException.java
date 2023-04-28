package com.flink.plugins.inf.exception;

import lombok.ToString;

/**
 * @fileName: BaseException.java
 * @description: 基础异常类
 * @author: huangshimin
 * @date: 2022/9/30 4:12 PM
 */
@ToString
public class BaseException extends RuntimeException {

    public BaseException(String errorMsg) {
        super(errorMsg);
    }

    public BaseException(String errorMsg, Throwable originalException) {
        super(errorMsg, originalException);
    }

    public BaseException(ExceptionEnums exceptionEnums) {
        super(exceptionEnums.getMsg());
    }
}
