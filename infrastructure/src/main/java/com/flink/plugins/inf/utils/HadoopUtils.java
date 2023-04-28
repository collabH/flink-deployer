package com.flink.plugins.inf.utils;

import com.flink.plugins.inf.exception.ConfigurationException;
import com.flink.plugins.inf.exception.ExceptionEnums;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Set;

/**
 * @fileName: HadoopUtils.java
 * @description: hadoop工具类
 * @author: huangshimin
 * @date: 2022/9/30 3:34 PM
 */
public class HadoopUtils {
    private final static Set<String> HADOOP_CONFIG_NAME_LIST = Sets.newHashSet("hdfs-site", "core-site", "yarn-site");

    /**
     * 校验hadoop配置合法性
     *
     * @param hadoopConfigPathList hadoop配置路径列表
     */
    public static void validateConfig(List<String> hadoopConfigPathList) {
        if (CollectionUtils.isNotEmpty(hadoopConfigPathList)) {
            for (String hadoopConfigPath : hadoopConfigPathList) {
                if (!hadoopConfigPath.endsWith(".xml")) {
                    throw new ConfigurationException(ExceptionEnums.CONFIG_ERROR_SUFFIX);
                }
                if (!HADOOP_CONFIG_NAME_LIST.contains(hadoopConfigPath)) {
                    throw new ConfigurationException(ExceptionEnums.CONFIG_NOT_SUPPORT);
                }
            }
        }
    }
}
