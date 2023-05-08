package com.flink.plugins.inf.deployer;


import com.flink.plugins.inf.config.CheckpointConfig;
import com.flink.plugins.inf.config.CoreConfig;
import com.flink.plugins.inf.config.DeploymentConfig;
import com.flink.plugins.inf.config.JobManagerConfig;
import com.flink.plugins.inf.config.PipelineConfig;
import com.flink.plugins.inf.config.RuntimeConfig;
import com.flink.plugins.inf.config.TaskManagerConfig;
import com.flink.plugins.inf.config.YarnClusterDescriptorConfig;
import com.google.common.collect.Lists;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Before;
import org.junit.Test;

/**
 * @fileName: ClusterDeployerFactoryTest.java
 * @description: ClusterDeployerFactoryTest.java类说明
 * @author: huangshimin
 * @date: 2023/5/8 16:55
 */
public class ClusterDeployerFactoryTest {
    private YarnClusterDescriptor yarnClusterDescriptor;

    @Before
    public void obtainYarnClusterDescriptor() {
        YarnClusterDescriptorConfig yarnClusterDescriptorConfig = new YarnClusterDescriptorConfig();
        yarnClusterDescriptorConfig.setLocalJarPath(
                "/Users/huangshimin/Documents/study/flink/flink-1.16.1/lib/flink-dist-1.16.1.jar");
        yarnClusterDescriptorConfig.setFlinkConfDir("/Users/huangshimin/Documents/study/flink/flink-1.16.1/conf/");
        yarnClusterDescriptorConfig.setShipFiles(Lists.newArrayList("/Users/huangshimin/Documents/study/flink/flink-1.16.1/lib"));
        yarnClusterDescriptorConfig.setHadoopConfList(Lists.newArrayList("file:///Users/huangshimin/Documents/dev/soft" +
                        "/hadoop330/etc/hadoop/yarn-site.xml",
                "file:///Users/huangshimin/Documents/dev/soft/hadoop330/etc/hadoop/hdfs-site.xml",
                "file:///Users/huangshimin/Documents/dev/soft/hadoop330/etc/hadoop/core-site.xml"));
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig();
        taskManagerConfig.setTotalProcessMemoryMb(2048L);
        taskManagerConfig.setCpuCores(2.0);
        taskManagerConfig.setNumTaskSlot(2);

        PipelineConfig pipelineConfig = new PipelineConfig();
        pipelineConfig.setJobName("test-flink-deployer");
        pipelineConfig.setJobJar(Lists.newArrayList("file:///Users/huangshimin/Documents/study/flink/flink-1.16.1/examples/streaming/WordCount.jar"));
        pipelineConfig.setMaxParallelism(1000);

        JobManagerConfig jobManagerConfig = new JobManagerConfig();
        jobManagerConfig.setTotalProcessMemoryMb(1024L);

        CoreConfig coreConfig = new CoreConfig();
        coreConfig.setHadoopConfDir("file:///Users/huangshimin/Documents/dev/soft/hadoop330/etc/hadoop");

        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setCheckpointDirectory("file:///Users/huangshimin/Documents/study/flink/checkpoint");
        checkpointConfig.setSavepointDirectory("file:///Users/huangshimin/Documents/study/flink/savepoint");
        checkpointConfig.setIncrementalCheckpoints(true);
        checkpointConfig.setCheckpointStorage("jobmanager");

        DeploymentConfig deploymentConfig = new DeploymentConfig();
        deploymentConfig.setExecutionAttached(true);

        RuntimeConfig runtimeConfig = RuntimeConfig.builder()
                .jobConfig(pipelineConfig)
                .taskManagerConfig(taskManagerConfig)
                .jobManagerConfig(jobManagerConfig)
                .coreConfig(coreConfig)
                .checkpointConfig(checkpointConfig)
                .deploymentConfig(deploymentConfig)
                .build();
        yarnClusterDescriptor =
                ClusterDeployerFactory.obtainYarnClusterDescriptor(yarnClusterDescriptorConfig, runtimeConfig);
    }

    @Test
    public void launchJob() throws ClusterDeploymentException {
        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .createClusterSpecification();
        ClusterClientProvider<ApplicationId> applicationIdClusterClientProvider =
                yarnClusterDescriptor.deployApplicationCluster(clusterSpecification,
                        new ApplicationConfiguration(new String[]{},
                                "org.apache.flink.streaming.examples.wordcount.WordCount"));
        String webInterfaceURL = applicationIdClusterClientProvider.getClusterClient()
                .getWebInterfaceURL();
        System.out.println(webInterfaceURL);
    }

}