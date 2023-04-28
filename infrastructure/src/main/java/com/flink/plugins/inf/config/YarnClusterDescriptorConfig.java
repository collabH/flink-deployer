package com.flink.plugins.inf.config;

import com.flink.plugins.inf.exception.BaseException;
import com.flink.plugins.inf.exception.ConfigurationException;
import com.flink.plugins.inf.utils.PreconditionUtils;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * @fileName: YarnClusterDescriptorConfig.java
 * @description: yarn ClusterDescriptor配置
 * @author: huangshimin
 * @date: 2022/9/30 5:06 PM
 */
@Data
public class YarnClusterDescriptorConfig {
    private List<File> shipFiles;
    private String localJarPath;
    private List<String> hadoopConfList;
    private String flinkConfDir;
    private Properties dynamicFlinkConf;

    public void validate() {
        PreconditionUtils.checkArgument(StringUtils.isNotEmpty(this.localJarPath), new BaseException("localJarPath" +
                "不能为空!"));
        PreconditionUtils.checkArgument(CollectionUtils.isNotEmpty(this.hadoopConfList),
                new ConfigurationException("hadoop配置文件路径不能为空!"));
        PreconditionUtils.checkArgument(StringUtils.isNotEmpty(flinkConfDir),new ConfigurationException("flink" +
                "配置文件目录不能为空!"));
    }
}
