package com.flink.plugins.inf.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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
     * @param configKey        配置key
     * @param configValue      配置值
     * @param dynamicFlinkConf 动态配置
     */
    public static void stringConfigValid(String configKey, String configValue, Configuration dynamicFlinkConf) {
        if (StringUtils.isNotEmpty(configValue)) {
            dynamicFlinkConf.setString(configKey, configValue);
        }
    }

    /**
     * List集合配置校验
     *
     * @param configKey        配置key
     * @param configValue      配置值
     * @param dynamicFlinkConf 动态配置
     */
    public static <T> void listConfigValid(ConfigOption<List<T>> configKey, List<T> configValue,
                                           Configuration dynamicFlinkConf) {
        if (CollectionUtils.isNotEmpty(configValue)) {
            dynamicFlinkConf.set(configKey, configValue);
        }
    }

    /**
     * map集合配置校验
     *
     * @param configKey        配置key
     * @param configValue      配置值
     * @param dynamicFlinkConf 动态配置
     */
    public static <KEY,VALUE> void mapConfigValid(ConfigOption<Map<KEY,VALUE>> configKey,Map<KEY,VALUE> configValue,
                                          Configuration dynamicFlinkConf) {
        if (MapUtils.isNotEmpty(configValue)) {
            dynamicFlinkConf.set(configKey, configValue);
        }
    }

    /**
     * obj配置校验
     *
     * @param configKey        配置key
     * @param configValue      配置值
     * @param dynamicFlinkConf 动态配置
     * @param <T>              obj类型
     */
    public static <T> void objConfigValid(ConfigOption<T> configKey, T configValue, Configuration dynamicFlinkConf) {
        if (Objects.nonNull(configValue)) {
            dynamicFlinkConf.set(configKey, configValue);
        }
    }

    /**
     * memorySize对象配置校验
     *
     * @param configKey        配置key
     * @param memorySize       mb内存配置
     * @param dynamicFlinkConf 动态配置
     */
    public static void memorySizeValid(ConfigOption<MemorySize> configKey, Long memorySize,
                                       Configuration dynamicFlinkConf) {
        if (Objects.nonNull(memorySize)) {
            dynamicFlinkConf.set(configKey, MemorySize.ofMebiBytes(memorySize));
        }
    }
}
