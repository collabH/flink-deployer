package com.flink.plugins.inf.descriptor;

import com.flink.plugins.inf.config.YarnClusterDescriptorConfig;
import com.flink.plugins.inf.utils.YarnClientUtils;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.List;

/**
 * @fileName: FlinkYarnClusterDescriptor.java
 * @description: flink yarnClusterDescriptor
 * @author: huangshimin
 * @date: 2022/9/30 4:38 PM
 */
public class FlinkYarnClusterDescriptor implements FlinkClusterDescriptor<ApplicationId, YarnClusterDescriptorConfig> {
    @Override
    public ClusterDescriptor<ApplicationId> buildClusterDescriptor(YarnClusterDescriptorConfig clusterConfig) {
        clusterConfig.validate();
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
        List<String> hadoopConfList = clusterConfig.getHadoopConfList();
        YarnClient yarnClient = YarnClientUtils.getYarnClient(hadoopConfList);
        YarnConfiguration yarnConfiguration = YarnClientUtils.getYarnConfiguration(hadoopConfList);
        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                configuration, yarnConfiguration, yarnClient,
                YarnClientYarnClusterInformationRetriever.create(yarnClient), true);
        yarnClusterDescriptor.addShipFiles(clusterConfig.getShipFiles());
        yarnClusterDescriptor.setLocalJarPath(clusterConfig.getLocalJarPath());
        return yarnClusterDescriptor;
    }
}
