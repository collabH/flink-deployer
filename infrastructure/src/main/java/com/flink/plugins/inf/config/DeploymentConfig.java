package com.flink.plugins.inf.config;

import com.flink.plugins.inf.constants.TargetTypeEnums;
import com.flink.plugins.inf.utils.ConfigValidateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;

import java.util.List;

/**
 * @fileName: DeploymentConfig.java
 * @description: flink部署配置
 * @author: huangshimin
 * @date: 2023/4/28 15:44
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DeploymentConfig {
    /**
     * flink部署方式，支持remote、local、yarn-per-job (deprecated)、yarn-session、kubernetes-session、yarn-application
     * kubernetes-application, 默认为yarn-application
     */
    private String executionTarget = TargetTypeEnums.YARN_APPLICATION.getTarget();

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
     * @param dynamicFlinkConf flink动态配置
     */
    public void buildDeploymentProperties(Configuration dynamicFlinkConf) {
        dynamicFlinkConf.set(DeploymentOptions.TARGET, this.executionTarget);
        dynamicFlinkConf.set(DeploymentOptions.ATTACHED, this.executionAttached);
        ConfigValidateUtils.listConfigValid(DeploymentOptions.JOB_LISTENERS, this.executionJobListeners,
                dynamicFlinkConf);
    }

}
