package com.flink.plugins.inf.deployer;


import com.flink.plugins.inf.config.CheckpointConfig;
import com.flink.plugins.inf.config.CoreConfig;
import com.flink.plugins.inf.config.DeploymentConfig;
import com.flink.plugins.inf.config.JobManagerConfig;
import com.flink.plugins.inf.config.PipelineConfig;
import com.flink.plugins.inf.config.RuntimeConfig;
import com.flink.plugins.inf.config.TaskManagerConfig;
import com.flink.plugins.inf.config.kubernetes.KubernetesConfig;
import com.flink.plugins.inf.config.yarn.YarnConfig;
import com.flink.plugins.inf.constants.TargetTypeEnums;
import com.google.common.collect.Lists;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Test;

/**
 * @fileName: ClusterDeployerFactoryTest.java
 * @description: ClusterDeployerFactoryTest.java类说明
 * @author: huangshimin
 * @date: 2023/5/8 16:55
 */
public class ClusterDeployerFactoryTest {

    private KubernetesClusterDescriptor obtainKubernetesClusterDescriptor() {
        KubernetesConfig kubernetesConfig = new KubernetesConfig();
        kubernetesConfig.setClusterId("test-k8s-deployer");
        kubernetesConfig.setServiceAccount("flink");
        kubernetesConfig.setNamespace("flink-native-kubernetes");
        kubernetesConfig.setContext("docker-desktop");
        kubernetesConfig.setJobFlinkConfDir("/Users/huangshimin/Documents/study/flink/flink-1.16.1/conf/");
        kubernetesConfig.setKubeConfigFile("/Users/huangshimin/.kube/config");
        RuntimeConfig runtimeConfig = buildK8sFlinkRuntimeConfig();
        return ClusterDeployerFactory.obtainKubernetesClusterDescriptor(kubernetesConfig, runtimeConfig);
    }

    private YarnClusterDescriptor obtainYarnClusterDescriptor() {
        YarnConfig yarnConfig = new YarnConfig();
        yarnConfig.setLocalJarPath(
                "/Users/huangshimin/Documents/study/flink/flink-1.16.1/lib/flink-dist-1.16.1.jar");
        yarnConfig.setJobFlinkConfDir("/Users/huangshimin/Documents/study/flink/flink-1.16.1/conf/");
        yarnConfig.setShipFiles(Lists.newArrayList("/Users/huangshimin/Documents/study/flink/flink-1.16.1/lib"));
        yarnConfig.setHadoopConfList(Lists.newArrayList("file:///Users/huangshimin/Documents/dev/soft" +
                        "/hadoop330/etc/hadoop/yarn-site.xml",
                "file:///Users/huangshimin/Documents/dev/soft/hadoop330/etc/hadoop/hdfs-site.xml",
                "file:///Users/huangshimin/Documents/dev/soft/hadoop330/etc/hadoop/core-site.xml"));
        RuntimeConfig runtimeConfig = buildYarnFlinkRuntimeConfig();
        return ClusterDeployerFactory.obtainYarnClusterDescriptor(yarnConfig, runtimeConfig);
    }


    private RuntimeConfig buildYarnFlinkRuntimeConfig() {
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig();
        taskManagerConfig.setTotalProcessMemoryMb(2048L);
        taskManagerConfig.setCpuCores(2.0);
        taskManagerConfig.setNumTaskSlot(2);

        PipelineConfig pipelineConfig = new PipelineConfig();
        pipelineConfig.setJobName("test-flink-deployer");
        pipelineConfig.setJobJar(Lists.newArrayList(
                "file:///Users/huangshimin/Documents/study/flink/flink-1.16.1/examples/streaming/WordCount.jar"));
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
        return RuntimeConfig.builder()
                .jobConfig(pipelineConfig)
                .taskManagerConfig(taskManagerConfig)
                .jobManagerConfig(jobManagerConfig)
                .coreConfig(coreConfig)
                .checkpointConfig(checkpointConfig)
                .deploymentConfig(deploymentConfig)
                .build();
    }


    private RuntimeConfig buildK8sFlinkRuntimeConfig() {
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig();
        taskManagerConfig.setTotalProcessMemoryMb(2048L);
        taskManagerConfig.setCpuCores(2.0);
        taskManagerConfig.setNumTaskSlot(2);

        PipelineConfig pipelineConfig = new PipelineConfig();
        pipelineConfig.setJobName("test-flink-deployer");
        pipelineConfig.setJobJar(Lists.newArrayList(
                "local:///opt/flink/examples/streaming/WordCount.jar"));
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
        deploymentConfig.setExecutionTarget(TargetTypeEnums.KUBERNETES_APPLICATION.getTarget());
        return RuntimeConfig.builder()
                .jobConfig(pipelineConfig)
                .taskManagerConfig(taskManagerConfig)
                .jobManagerConfig(jobManagerConfig)
                .coreConfig(coreConfig)
                .checkpointConfig(checkpointConfig)
                .deploymentConfig(deploymentConfig)
                .build();
    }

    @Test
    public void launchJobOnYarn() throws ClusterDeploymentException {
        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .createClusterSpecification();
        ClusterClientProvider<ApplicationId> applicationIdClusterClientProvider =
                obtainYarnClusterDescriptor().deployApplicationCluster(clusterSpecification,
                        new ApplicationConfiguration(new String[]{},
                                "org.apache.flink.streaming.examples.wordcount.WordCount"));
        String webInterfaceURL = applicationIdClusterClientProvider.getClusterClient()
                .getWebInterfaceURL();
        System.out.println(webInterfaceURL);
    }

    @Test
    public void launchJobOnK8s() throws ClusterDeploymentException {
        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .createClusterSpecification();
        ClusterClientProvider<String> clusterClientProvider =
                obtainKubernetesClusterDescriptor().deployApplicationCluster(clusterSpecification,
                        new ApplicationConfiguration(new String[]{},
                                "org.apache.flink.streaming.examples.wordcount.WordCount"));
        ClusterClient<String> clusterClient = clusterClientProvider.getClusterClient();
        String webInterfaceURL = clusterClient.getWebInterfaceURL();
        String clusterId = clusterClient.getClusterId();
        System.out.println(webInterfaceURL + "---" + clusterId);
    }

}