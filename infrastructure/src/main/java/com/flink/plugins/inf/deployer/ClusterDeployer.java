package com.flink.plugins.inf.deployer;

import org.apache.flink.client.deployment.ClusterDescriptor;

/**
 * @fileName: ClusterDeployer.java
 * @description: flink ClusterDeployer基础类
 * @author: huangshimin
 * @date: 2022/9/30 4:31 PM
 */
public interface ClusterDeployer<ClusterType, Config> {

    /**
     * 构建clusterDescriptor
     *
     * @param clusterConfig clusterDescriptor所需配置
     * @return clusterDescriptor基础类
     */
    ClusterDescriptor<ClusterType> buildClusterDescriptor(Config clusterConfig);
}
