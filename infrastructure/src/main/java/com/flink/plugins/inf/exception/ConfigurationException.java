package com.flink.plugins.inf.exception;

/**
 * @fileName: ConfigurationException.java
 * @description: 配置相关自定义异常
 * @author: huangshimin
 * @date: 2022/9/30 3:42 PM
 */
public class ConfigurationException extends BaseException {

    public ConfigurationException(ExceptionEnums exceptionEnums) {
        super(exceptionEnums);
    }

    public ConfigurationException(String msg){
        super(msg);
    }
}
