package com.flink.plugins.inf.config;

import com.flink.plugins.inf.exception.BaseException;
import com.flink.plugins.inf.utils.ConfigValidateUtils;
import com.flink.plugins.inf.utils.PreconditionUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @fileName: PipelineConfig.java
 * @description: flink任务相关配置
 * @author: huangshimin
 * @date: 2023/5/8 19:38
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipelineConfig {
    private String jobName;
    private List<String> jobJar;
    private List<String> classpath;
    private Long watermarkIntervalMs = 200L;
    private Boolean forceAvro = false;
    private Boolean forceKryo = false;
    private Map<String, String> globalJobParameters;
    private Integer maxParallelism = -1;
    private Boolean operatorChaining = true;

    /**
     * 构建flink任务配置
     *
     * @param dynamicFlinkConf flink动态配置
     */
    public void buildPipelineProperties(Configuration dynamicFlinkConf) {
        ConfigValidateUtils.stringConfigValid(PipelineOptions.NAME.key(), this.jobName,
                dynamicFlinkConf);
        PreconditionUtils.checkArgument(CollectionUtils.isNotEmpty(this.jobJar),
                new BaseException("flink job jar不能为空!"));
        dynamicFlinkConf.set(PipelineOptions.JARS, this.jobJar);
        ConfigValidateUtils.listConfigValid(PipelineOptions.CLASSPATHS, this.classpath, dynamicFlinkConf);
        dynamicFlinkConf.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(this.watermarkIntervalMs));
        dynamicFlinkConf.set(PipelineOptions.FORCE_AVRO, this.forceAvro);
        dynamicFlinkConf.set(PipelineOptions.FORCE_KRYO, this.forceKryo);
        ConfigValidateUtils.mapConfigValid(PipelineOptions.GLOBAL_JOB_PARAMETERS, this.globalJobParameters,
                dynamicFlinkConf);
        dynamicFlinkConf.set(PipelineOptions.MAX_PARALLELISM, this.maxParallelism);
        dynamicFlinkConf.set(PipelineOptions.OPERATOR_CHAINING, this.operatorChaining);
    }
}
