package com.flink.plugins.inf.utils;

/**
 * @fileName: PreconditionUtils.java
 * @description: 校验工具类
 * @author: huangshimin
 * @date: 2023/4/27 20:28
 */
public class PreconditionUtils {

    /**
     * 校验表达式是否合法
     *
     * @param expression 校验表达式，true为合法，false为非法
     * @param exception  自定义异常
     */
    public static void checkArgument(boolean expression, RuntimeException exception) {
        if (!expression) {
            throw exception;
        }
    }

    /**
     * 校验对象是否为空
     *
     * @param reference 被校验对象
     * @param exception 自定义异常
     * @param <T>       对象类型
     * @return 非空对象
     */
    public static <T> T checkNotNull(T reference, RuntimeException exception) {
        if (reference == null) {
            throw exception;
        } else {
            return reference;
        }
    }
}
