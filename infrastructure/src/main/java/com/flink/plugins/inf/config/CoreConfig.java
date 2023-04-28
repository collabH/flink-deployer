package com.flink.plugins.inf.config;

import com.flink.plugins.inf.utils.ConfigValidateUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.CoreOptions;

import java.util.Properties;

/**
 * @fileName: CoreConfig.java
 * @description: flink核心配置
 * @author: huangshimin
 * @date: 2023/4/28 15:07
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class CoreConfig {
    /**
     * 默认并行度，默认为1
     */
    private Integer defaultParallelism = 1;
    /**
     * 类加载顺序，默认为child-first
     */
    private String classloaderResolveOrder = "child-first";
    /**
     * flink jvm参数配置通过-D方式指定
     */
    private String JvmOptions = "";
    /**
     * flink jobManger jvm参数配置通过-D指定
     */
    private String jmJvmOptions = "";
    /**
     * flink taskManger jvm参数配置通过-D指定
     */
    private String tmJvmOptions = "";
    /**
     * flink client jvm参数配置通过-D指定
     */
    private String cliJvmOptions = "";

    /**
     * hadoop配置目录
     */
    private String hadoopConfDir;
    /**
     * hbase配置目录
     */
    private String hbaseConfDir;

    /**
     * 构建core配置
     * @param dynamicProperties flink动态配置
     */
    public void buildCoreProperties(Properties dynamicProperties) {
        dynamicProperties.put(CoreOptions.DEFAULT_FILESYSTEM_SCHEME.key(), this.defaultParallelism);
        dynamicProperties.put(CoreOptions.FLINK_JVM_OPTIONS.key(), this.JvmOptions);
        dynamicProperties.put(CoreOptions.FLINK_JM_JVM_OPTIONS.key(), this.jmJvmOptions);
        dynamicProperties.put(CoreOptions.FLINK_TM_JVM_OPTIONS.key(), this.tmJvmOptions);
        dynamicProperties.put(CoreOptions.FLINK_CLI_JVM_OPTIONS.key(), this.cliJvmOptions);
        ConfigValidateUtils.stringConfigValid(CoreOptions.FLINK_HADOOP_CONF_DIR.key(), this.hadoopConfDir,
                dynamicProperties);
        ConfigValidateUtils.stringConfigValid(CoreOptions.FLINK_HADOOP_CONF_DIR.key(),
                this.hbaseConfDir,
                dynamicProperties);
    }

}
