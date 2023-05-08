package com.flink.plugins.inf.deployer;

import com.flink.plugins.inf.config.RuntimeConfig;
import com.flink.plugins.inf.config.YarnClusterDescriptorConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.YarnClusterDescriptor;

/**
 * @fileName: ClusterDeployerFactory.java
 * @description: 集群部署器工厂
 * @author: huangshimin
 * @date: 2023/5/8 16:08
 */
public class ClusterDeployerFactory {

    /**
     * 获取yarn集群描述符
     *
     * @param yarnClusterDescriptorConfig yarn配置
     * @param runtimeConfig               flink运行配置
     * @return {@link YarnClusterDescriptor}
     */
    public static YarnClusterDescriptor obtainYarnClusterDescriptor(YarnClusterDescriptorConfig
                                                                            yarnClusterDescriptorConfig,
                                                                    RuntimeConfig runtimeConfig) {
        YarnClusterDeployer yarnClusterDeployer = new YarnClusterDeployer();
        Configuration dynamicFlinkConf = new Configuration();
        buildDynamicFlinkConf(dynamicFlinkConf, runtimeConfig);
        yarnClusterDescriptorConfig.setDynamicFlinkConf(dynamicFlinkConf);
        return (YarnClusterDescriptor) yarnClusterDeployer.buildClusterDescriptor(yarnClusterDescriptorConfig);
    }

    /**
     * 构建动态flink配置
     *
     * @param dynamicFlinkConf 动态flink配置
     * @param runtimeConfig    运行配置
     */
    private static void buildDynamicFlinkConf(Configuration dynamicFlinkConf, RuntimeConfig runtimeConfig) {
        runtimeConfig.getCoreConfig().buildCoreProperties(dynamicFlinkConf);
        runtimeConfig.getJobConfig().buildPipelineProperties(dynamicFlinkConf);
        runtimeConfig.getCheckpointConfig().buildCheckpointProperties(dynamicFlinkConf);
        runtimeConfig.getDeploymentConfig().buildDeploymentProperties(dynamicFlinkConf);
        runtimeConfig.getJobManagerConfig().buildJobManagerProperties(dynamicFlinkConf);
        runtimeConfig.getTaskManagerConfig().buildTaskManagerProperties(dynamicFlinkConf);
    }
}
