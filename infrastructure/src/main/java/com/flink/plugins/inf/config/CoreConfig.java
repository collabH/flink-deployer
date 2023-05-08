package com.flink.plugins.inf.config;

import com.flink.plugins.inf.utils.ConfigValidateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;

/**
 * @fileName: CoreConfig.java
 * @description: flink核心配置
 * @author: huangshimin
 * @date: 2023/4/28 15:07
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
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
     *
     * @param dynamicFlinkConf flink动态配置
     */
    public void buildCoreProperties(Configuration dynamicFlinkConf) {
        dynamicFlinkConf.set(CoreOptions.DEFAULT_PARALLELISM, this.defaultParallelism);
        dynamicFlinkConf.set(CoreOptions.FLINK_JVM_OPTIONS, this.JvmOptions);
        dynamicFlinkConf.set(CoreOptions.FLINK_JM_JVM_OPTIONS, this.jmJvmOptions);
        dynamicFlinkConf.set(CoreOptions.FLINK_TM_JVM_OPTIONS, this.tmJvmOptions);
        dynamicFlinkConf.set(CoreOptions.FLINK_CLI_JVM_OPTIONS, this.cliJvmOptions);
        ConfigValidateUtils.stringConfigValid(CoreOptions.FLINK_HADOOP_CONF_DIR.key(), this.hadoopConfDir,
                dynamicFlinkConf);
        ConfigValidateUtils.stringConfigValid(CoreOptions.FLINK_HADOOP_CONF_DIR.key(),
                this.hbaseConfDir,
                dynamicFlinkConf);
    }

}
