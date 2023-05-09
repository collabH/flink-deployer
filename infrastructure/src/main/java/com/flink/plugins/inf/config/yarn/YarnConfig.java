package com.flink.plugins.inf.config.yarn;

import com.flink.plugins.inf.config.FlinkConfig;
import com.flink.plugins.inf.exception.ConfigurationException;
import com.flink.plugins.inf.utils.PreconditionUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import java.util.List;

/**
 * @fileName: YarnClusterDescriptorConfig.java
 * @description: yarn ClusterDescriptor配置
 * @author: huangshimin
 * @date: 2022/9/30 5:06 PM
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class YarnConfig extends FlinkConfig {
    private List<String> shipFiles;
    private String localJarPath;
    private List<String> hadoopConfList;

    public void validate() {
        super.validate();
        PreconditionUtils.checkArgument(CollectionUtils.isNotEmpty(this.shipFiles),
                new ConfigurationException("shipFiles不能为空!"));
        super.getDynamicFlinkConf().set(YarnConfigOptions.SHIP_FILES, this.shipFiles);
        PreconditionUtils.checkArgument(StringUtils.isNotEmpty(this.localJarPath),
                new ConfigurationException("localJarPath不能为空!"));
        PreconditionUtils.checkArgument(CollectionUtils.isNotEmpty(this.hadoopConfList),
                new ConfigurationException("hadoop配置文件路径不能为空!"));
    }
}
