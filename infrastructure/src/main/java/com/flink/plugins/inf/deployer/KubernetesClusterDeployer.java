package com.flink.plugins.inf.deployer;

import com.flink.plugins.inf.config.kubernetes.KubernetesConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;

/**
 * @fileName: KubernetesClusterDeployer.java
 * @description: flink k8s集群部署器
 * @author: huangshimin
 * @date: 2023/4/28 11:04
 */
public class KubernetesClusterDeployer implements ClusterDeployer<String, KubernetesConfig> {
    /**
     * 构建KubernetesClusterDescriptor
     * @param clusterConfig clusterDescriptor所需配置
     * @return {@link ClusterDescriptor<String>}
     */
    @Override
    public ClusterDescriptor<String> buildClusterDescriptor(KubernetesConfig clusterConfig) {
        clusterConfig.validate();
        String flinkConfDir = clusterConfig.getJobFlinkConfDir();
        Configuration configuration;
        if (StringUtils.isNotEmpty(flinkConfDir)) {
            configuration = GlobalConfiguration.loadConfiguration(flinkConfDir,
                    clusterConfig.getDynamicFlinkConf());
        } else {
            configuration = GlobalConfiguration.loadConfiguration(
                    clusterConfig.getDynamicFlinkConf());
        }
        FlinkKubeClient flinkKubeClient = FlinkKubeClientFactory.getInstance()
                .fromConfiguration(configuration, clusterConfig.getUseCase());
        return new KubernetesClusterDescriptor(
                configuration, flinkKubeClient);
    }
}
