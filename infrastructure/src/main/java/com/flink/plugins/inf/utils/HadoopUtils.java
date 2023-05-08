package com.flink.plugins.inf.utils;

import com.flink.plugins.inf.exception.ConfigurationException;
import com.flink.plugins.inf.exception.ExceptionEnums;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @fileName: HadoopUtils.java
 * @description: hadoop工具类
 * @author: huangshimin
 * @date: 2022/9/30 3:34 PM
 */
public class HadoopUtils {

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
                if (!StringUtils.containsAny(hadoopConfigPath, "hdfs-site", "core-site", "yarn-site")) {
                    throw new ConfigurationException(ExceptionEnums.CONFIG_NOT_SUPPORT);
                }
            }
        }
    }
}
