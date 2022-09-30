package com.flink.plugins.inf.descriptor;

import org.apache.flink.client.deployment.ClusterDescriptor;

/**
 * @fileName: AbstractClusterDescriptor.java
 * @description: flink clusterDescriptor基础类
 * @author: huangshimin
 * @date: 2022/9/30 4:31 PM
 */
public interface FlinkClusterDescriptor<ClusterType, Config> {

    /**
     * 构建clusterDescriptor
     *
     * @param clusterConfig clusterDescriptor所需配置
     * @return clusterDescriptor基础类
     */
    abstract ClusterDescriptor<ClusterType> buildClusterDescriptor(Config clusterConfig);
}
