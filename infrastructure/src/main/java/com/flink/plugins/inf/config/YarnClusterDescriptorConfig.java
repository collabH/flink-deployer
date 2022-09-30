package com.flink.plugins.inf.config;

import lombok.Data;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.List;

/**
 * @fileName: YarnClusterDescriptorConfig.java
 * @description: yarn ClusterDescriptor配置
 * @author: huangshimin
 * @date: 2022/9/30 5:06 PM
 */
@Data
public class YarnClusterDescriptorConfig {
    private List<File> shipFiles;
    private Path localJarPath;
    private List<String> hadoopConfList;

    public void validate() {
        // todo 校验参数
    }
}
