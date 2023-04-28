package com.flink.plugins.inf.config;

import com.flink.plugins.inf.utils.ConfigValidateUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.DeploymentOptions;

import java.util.List;
import java.util.Properties;

/**
 * @fileName: DeploymentConfig.java
 * @description: flink部署配置
 * @author: huangshimin
 * @date: 2023/4/28 15:44
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DeploymentConfig {
    /**
     * flink部署方式，支持remote、local、yarn-per-job (deprecated)、yarn-session、kubernetes-session、yarn-application
     * kubernetes-application, 默认为yarn-application
     */
    private String executionTarget = "yarn-application";

    /**
     * 启动方式是否后台方式，日志不会打印在client上，默认为false
     */
    private Boolean executionAttached = false;

    /**
     * job监听器
     */
    private List<String> executionJobListeners;

    /**
     * 构建deployment配置
     *
     * @param dynamicProperties flink动态配置
     */
    public void buildCheckpointProperties(Properties dynamicProperties) {
        dynamicProperties.put(DeploymentOptions.TARGET.key(), this.executionTarget);
        dynamicProperties.put(DeploymentOptions.ATTACHED.key(), this.executionAttached);
        ConfigValidateUtils.listConfigValid(DeploymentOptions.JOB_LISTENERS.key(), this.executionJobListeners,
                dynamicProperties);
    }

}
