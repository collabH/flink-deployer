package com.flink.plugins.inf.config.kubernetes;

import com.flink.plugins.inf.config.FlinkConfig;
import com.flink.plugins.inf.constants.KubernetesUseCaseEnums;
import com.flink.plugins.inf.exception.ConfigurationException;
import com.flink.plugins.inf.utils.ConfigValidateUtils;
import com.flink.plugins.inf.utils.PreconditionUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesHighAvailabilityOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @fileName: KubernetesConfig.java
 * @description: flink on k8s配置
 * @author: huangshimin
 * @date: 2023/5/9 10:33
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class KubernetesConfig extends FlinkConfig {
    /**
     * Kubernetes客户端用例支持client、resourcemanager、kubernetes-ha-services
     */
    private String useCase = KubernetesUseCaseEnums.KUBERNETES_HA_SERVICES.getUseCase();
    /**
     * 用于配置Kubernetes客户端的Kubernetes配置文件中的所需上下文用于与集群交互.
     */
    private String context;
    private String restServiceExposedType = KubernetesConfigOptions.ServiceExposedType.ClusterIP.name();
    private String restServiceExposedNodePortAddressType =
            KubernetesConfigOptions.NodePortAddressType.InternalIP.name();
    /**
     * jm和tm的serviceAccount
     */
    private String serviceAccount = "default";
    private String jobManagerServiceAccount = "default";
    private String taskManagerServiceAccount = "default";
    private List<Map<String, String>> jobManagerOwnerReference;
    private Double jobManagerCpu = 1.0;
    private Double jobManagerCpuLimitFactor = 1.0;
    private Double jobManagerMemoryLimitFactor = 1.0;
    private Double taskManagerCpu = -1.0;
    private Double taskManagerCpuLimitFactor = 1.0;
    private Double taskManagerMemoryLimitFactor = 1.0;
    private String containerImagePullPolicy = KubernetesConfigOptions.ImagePullPolicy.IfNotPresent.name();
    /**
     * 访问Kubernetes image镜像库密码，以分号分隔。
     */
    private List<String> containerImagePullSecrets;
    /**
     * Kubernetes config file文件，默认使用~/.kube/config
     */
    private String kubeConfigFile;
    private String namespace = "default";
    /**
     * jm pod设置的标签，例如version:alphav1,deploy:test.
     */
    private Map<String, String> jobMangerLabels;
    private Map<String, String> taskMangerLabels;
    /**
     * 为jm设置node selector，例如environment:production,disk:ssd.
     */
    private Map<String, String> jobManagerNodeSelector;
    private Map<String, String> taskManagerNodeSelector;
    private String clusterId;
    private String containerImage;
    private String kubernetesEntryPath = "/docker-entrypoint.sh";
    private String flinkConfDir = "/opt/flink/conf";
    private String flinkLogDir;
    private String hadoopConfConfigMap;
    /**
     * 为jm设置用户指定的注释，例如a1:v1,a2:v2
     */
    private Map<String, String> jobManagerAnnotations;
    private Map<String, String> taskManagerAnnotations;
    /**
     * jm pod启动参数
     */
    private String kubernetesJobmanagerEntrypointArgs = "";
    private String kubernetesTaskmanagerEntrypointArgs = "";
    /**
     * 为jm设置指定的Tolerations
     */
    private List<Map<String, String>> jobManagerTolerations;
    private List<Map<String, String>> taskManagerTolerations;
    /**
     * 为rest service指定别名，例如a1:v1,a2:v2
     */
    private Map<String, String> restServiceAnnotations;
    /**
     * 将挂载到Flink容器中的用户指定的Secrets。
     */
    private Map<String, String> kubernetesSecrets;
    private List<Map<String, String>> kubernetesEnvSecretKeyRef;
    private Integer clientIOExecutorPoolSize = 4;
    private Integer jobmanagerReplicas = 1;
    private Boolean hostNetworkEnabled = false;
    private String clientUserAgent = "flink";
    private Boolean hadoopConfMountEnabled = true;
    private Boolean kerberosMountEnabled = true;
    private String nodeNameLabel = "kubernetes.io/hostname";
    private String jobManagerPodTemplate;
    private String taskManagerPodTemplate;
    private String podTemplate;

    /**
     * ha config
     */
    private Integer leaseDurationSeconds = 15;
    private Integer renewDeadlineSeconds = 15;
    private Integer retryPeriodSeconds = 5;


    public void validate() {
        super.validate();
        PreconditionUtils.checkArgument(StringUtils.isNotEmpty(this.context),
                new ConfigurationException("context不能为空!"));
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.CONTEXT, this.context);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                KubernetesConfigOptions.ServiceExposedType.valueOf(this.restServiceExposedType));
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.REST_SERVICE_EXPOSED_NODE_PORT_ADDRESS_TYPE,
                KubernetesConfigOptions.NodePortAddressType.valueOf(this.restServiceExposedNodePortAddressType));
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_SERVICE_ACCOUNT, this.serviceAccount);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT,
                this.jobManagerServiceAccount);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.TASK_MANAGER_SERVICE_ACCOUNT,
                this.taskManagerServiceAccount);
        ConfigValidateUtils.listConfigValid(KubernetesConfigOptions.JOB_MANAGER_OWNER_REFERENCE,
                this.jobManagerOwnerReference, super.getDynamicFlinkConf());
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.JOB_MANAGER_CPU, this.jobManagerCpu);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.JOB_MANAGER_CPU_LIMIT_FACTOR,
                this.jobManagerCpuLimitFactor);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.JOB_MANAGER_MEMORY_LIMIT_FACTOR,
                this.jobManagerMemoryLimitFactor);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.TASK_MANAGER_CPU, this.taskManagerCpu);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.TASK_MANAGER_CPU_LIMIT_FACTOR,
                this.taskManagerCpuLimitFactor);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.TASK_MANAGER_MEMORY_LIMIT_FACTOR,
                this.taskManagerMemoryLimitFactor);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY,
                KubernetesConfigOptions.ImagePullPolicy.valueOf(this.containerImagePullPolicy));

        ConfigValidateUtils.listConfigValid(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS,
                this.containerImagePullSecrets, super.getDynamicFlinkConf());
        ConfigValidateUtils.stringConfigValid(KubernetesConfigOptions.KUBE_CONFIG_FILE.key(), this.kubeConfigFile,
                super.getDynamicFlinkConf());
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.NAMESPACE, this.namespace);
        ConfigValidateUtils.mapConfigValid(KubernetesConfigOptions.JOB_MANAGER_LABELS, this.jobMangerLabels,
                super.getDynamicFlinkConf());
        ConfigValidateUtils.mapConfigValid(KubernetesConfigOptions.TASK_MANAGER_LABELS, this.taskMangerLabels,
                super.getDynamicFlinkConf());
        ConfigValidateUtils.mapConfigValid(KubernetesConfigOptions.JOB_MANAGER_NODE_SELECTOR,
                this.jobManagerNodeSelector, super.getDynamicFlinkConf());
        ConfigValidateUtils.mapConfigValid(KubernetesConfigOptions.TASK_MANAGER_NODE_SELECTOR,
                this.taskManagerNodeSelector, super.getDynamicFlinkConf());
        PreconditionUtils.checkArgument(StringUtils.isNotEmpty(this.clusterId),
                new ConfigurationException("clusterId不能为空!"));
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.CLUSTER_ID, this.clusterId);
        ConfigValidateUtils.stringConfigValid(KubernetesConfigOptions.CONTAINER_IMAGE.key(), this.containerImage,
                super.getDynamicFlinkConf());
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH, this.kubernetesEntryPath);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.FLINK_CONF_DIR, this.flinkConfDir);
        ConfigValidateUtils.stringConfigValid(KubernetesConfigOptions.FLINK_LOG_DIR.key(), this.flinkLogDir,
                super.getDynamicFlinkConf());
        ConfigValidateUtils.stringConfigValid(KubernetesConfigOptions.HADOOP_CONF_CONFIG_MAP.key(),
                this.hadoopConfConfigMap, super.getDynamicFlinkConf());
        ConfigValidateUtils.mapConfigValid(KubernetesConfigOptions.JOB_MANAGER_ANNOTATIONS,
                this.jobManagerAnnotations, super.getDynamicFlinkConf());
        ConfigValidateUtils.mapConfigValid(KubernetesConfigOptions.TASK_MANAGER_ANNOTATIONS,
                this.taskManagerAnnotations, super.getDynamicFlinkConf());
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_ENTRYPOINT_ARGS,
                this.kubernetesJobmanagerEntrypointArgs);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_TASKMANAGER_ENTRYPOINT_ARGS,
                this.kubernetesTaskmanagerEntrypointArgs);
        ConfigValidateUtils.listConfigValid(KubernetesConfigOptions.JOB_MANAGER_TOLERATIONS,
                this.jobManagerTolerations, super.getDynamicFlinkConf());
        ConfigValidateUtils.listConfigValid(KubernetesConfigOptions.TASK_MANAGER_TOLERATIONS,
                this.taskManagerTolerations, super.getDynamicFlinkConf());
        ConfigValidateUtils.mapConfigValid(KubernetesConfigOptions.REST_SERVICE_ANNOTATIONS,
                this.restServiceAnnotations, super.getDynamicFlinkConf());
        ConfigValidateUtils.mapConfigValid(KubernetesConfigOptions.KUBERNETES_SECRETS,
                this.kubernetesSecrets, super.getDynamicFlinkConf());
        ConfigValidateUtils.listConfigValid(KubernetesConfigOptions.KUBERNETES_ENV_SECRET_KEY_REF,
                this.kubernetesEnvSecretKeyRef, super.getDynamicFlinkConf());
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_CLIENT_IO_EXECUTOR_POOL_SIZE,
                this.clientIOExecutorPoolSize);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_JOBMANAGER_REPLICAS,
                this.jobmanagerReplicas);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_HOSTNETWORK_ENABLED,
                this.hostNetworkEnabled);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_CLIENT_USER_AGENT, this.clientUserAgent);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_HADOOP_CONF_MOUNT_DECORATOR_ENABLED,
                this.hadoopConfMountEnabled);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_KERBEROS_MOUNT_DECORATOR_ENABLED,
                this.kerberosMountEnabled);
        super.getDynamicFlinkConf().set(KubernetesConfigOptions.KUBERNETES_NODE_NAME_LABEL, this.nodeNameLabel);
        ConfigValidateUtils.stringConfigValid(KubernetesConfigOptions.JOB_MANAGER_POD_TEMPLATE.key(),
                this.jobManagerPodTemplate, super.getDynamicFlinkConf());
        ConfigValidateUtils.stringConfigValid(KubernetesConfigOptions.TASK_MANAGER_POD_TEMPLATE.key(),
                this.taskManagerPodTemplate, super.getDynamicFlinkConf());
        ConfigValidateUtils.stringConfigValid(KubernetesConfigOptions.KUBERNETES_POD_TEMPLATE.key(),
                this.podTemplate, super.getDynamicFlinkConf());
        super.getDynamicFlinkConf().set(KubernetesHighAvailabilityOptions.KUBERNETES_LEASE_DURATION,
                Duration.ofSeconds(this.leaseDurationSeconds));
        super.getDynamicFlinkConf().set(KubernetesHighAvailabilityOptions.KUBERNETES_RENEW_DEADLINE,
                Duration.ofSeconds(this.renewDeadlineSeconds));
        super.getDynamicFlinkConf().set(KubernetesHighAvailabilityOptions.KUBERNETES_RETRY_PERIOD,
                Duration.ofSeconds(this.retryPeriodSeconds));
    }
}
