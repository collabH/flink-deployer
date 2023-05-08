package com.flink.plugins.inf.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @fileName: FlinkRuntimeConfig.java
 * @description: flink运行配置
 * @author: huangshimin
 * @date: 2023/4/28 11:19
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RuntimeConfig {

    /**
     * flink核心配置
     */
    private CoreConfig coreConfig;
    /**
     * 任务配置
     */
    private PipelineConfig jobConfig;
    /**
     * jobManager配置
     */
    private JobManagerConfig jobManagerConfig;
    /**
     * taskManger配置
     */
    private TaskManagerConfig taskManagerConfig;
    /**
     * checkpoint配置
     */
    private CheckpointConfig checkpointConfig;

    /**
     * flink部署器配置
     */
    private DeploymentConfig deploymentConfig;

}
