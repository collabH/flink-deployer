package com.flink.plugins.inf.deployer;

import com.flink.plugins.inf.config.YarnClusterDescriptorConfig;
import com.flink.plugins.inf.utils.YarnClientUtils;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.hadoop.fs.Path;
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
public class YarnClusterDeployer implements ClusterDeployer<ApplicationId, YarnClusterDescriptorConfig> {
    /**
     * 构建yarn cluster descriptor
     *
     * @param clusterConfig clusterDescriptor所需配置
     * @return
     */
    @Override
    public ClusterDescriptor<ApplicationId> buildClusterDescriptor(YarnClusterDescriptorConfig clusterConfig) {
        clusterConfig.validate();
        String flinkConfDir = clusterConfig.getFlinkConfDir();
        Configuration configuration = GlobalConfiguration.loadConfiguration(flinkConfDir,
                clusterConfig.getDynamicFlinkConf());
        List<String> hadoopConfList = clusterConfig.getHadoopConfList();
        YarnClient yarnClient = YarnClientUtils.getYarnClient(hadoopConfList);
        YarnConfiguration yarnConfiguration = YarnClientUtils.getYarnConfiguration(hadoopConfList);
        YarnClusterDescriptor yarnClusterDescriptor =
                new YarnClusterDescriptor(
                        configuration, yarnConfiguration, yarnClient,
                        YarnClientYarnClusterInformationRetriever.create(yarnClient), true);
        yarnClusterDescriptor.setLocalJarPath(new Path(clusterConfig.getLocalJarPath()));
        return yarnClusterDescriptor;
    }
}
