package com.flink.plugins.inf.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * @fileName: ConfigValidateUtils.java
 * @description: 配置校验工具类
 * @author: huangshimin
 * @date: 2023/4/28 15:02
 */
public class ConfigValidateUtils {
    /**
     * 字符串配置校验
     *
     * @param configKey         配置key
     * @param configValue       配置值
     * @param dynamicProperties 动态配置
     */
    public static void stringConfigValid(String configKey, String configValue, Properties dynamicProperties) {
        if (StringUtils.isNotEmpty(configValue)) {
            dynamicProperties.put(configKey, configValue);
        }
    }

    /**
     * List集合配置校验
     *
     * @param configKey         配置key
     * @param configValue       配置值
     * @param dynamicProperties 动态配置
     */
    public static <T> void listConfigValid(String configKey, List<T> configValue, Properties dynamicProperties) {
        if (CollectionUtils.isNotEmpty(configValue)) {
            dynamicProperties.put(configKey, configValue);
        }
    }

    /**
     * obj配置校验
     *
     * @param configKey         配置key
     * @param configValue       配置值
     * @param dynamicProperties 动态配置
     * @param <T>               obj类型
     */
    public static <T> void objConfigValid(String configKey, T configValue, Properties dynamicProperties) {
        if (Objects.nonNull(configValue)) {
            dynamicProperties.put(configKey, configValue);
        }
    }
}
