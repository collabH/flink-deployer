package com.flink.plugins.inf.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.List;

/**
 * @fileName: FlinkOnYarnUtils.java
 * @description: Flink On Yarn工具类
 * @author: huangshimin
 * @date: 2022/9/30 3:07 PM
 */
public class YarnClientUtils {

    /**
     * 获取yarn客户端
     *
     * @param hadoopConfList hadoop配置列表
     * @return {@link YarnClient}
     */
    public static YarnClient getYarnClient(List<String> hadoopConfList) {
        HadoopUtils.validateConfig(hadoopConfList);
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(getYarnConfiguration(hadoopConfList));
        yarnClient.start();
        return yarnClient;
    }


    /**
     * 获取yarn配置
     *
     * @param hadoopConfPathList hadoop配置列表
     * @return {@link Configuration}
     */
    public static YarnConfiguration getYarnConfiguration(List<String> hadoopConfPathList) {
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        if (CollectionUtils.isNotEmpty(hadoopConfPathList)) {
            for (String hadoopConfPath : hadoopConfPathList) {
                yarnConfiguration.addResource(new Path(hadoopConfPath));
            }
        }
        return yarnConfiguration;
    }
}
