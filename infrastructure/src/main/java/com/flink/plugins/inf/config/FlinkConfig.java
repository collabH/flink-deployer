package com.flink.plugins.inf.config;

import com.flink.plugins.inf.exception.ConfigurationException;
import com.flink.plugins.inf.utils.PreconditionUtils;
import lombok.Data;
import org.apache.flink.configuration.Configuration;

/**
 * @fileName: FlinkConfig.java
 * @description: flink通用配置
 * @author: huangshimin
 * @date: 2023/5/9 10:55
 */
@Data
public class FlinkConfig {
    private String jobFlinkConfDir;
    private Configuration dynamicFlinkConf;

    public void validate() {
        PreconditionUtils.checkNotNull(this.dynamicFlinkConf, new ConfigurationException("flinkConf不能为空!"));
    }
}
