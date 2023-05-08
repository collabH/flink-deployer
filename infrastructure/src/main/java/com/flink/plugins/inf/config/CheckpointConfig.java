package com.flink.plugins.inf.config;

import com.flink.plugins.inf.utils.ConfigValidateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;

import java.util.Properties;

/**
 * @fileName: CheckpointConfig.java
 * @description: checkpoint配置
 * @author: huangshimin
 * @date: 2023/4/28 11:27
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CheckpointConfig {

    /**
     * savepoint存储目录
     */
    private String savepointDirectory;
    /**
     * checkpoint存储目录
     */
    private String checkpointDirectory;
    /**
     * state存储介质，jobmanager、filesystem
     */
    private String checkpointStorage;
    /**
     * checkpoint保留的个数，默认为1
     */
    private Integer checkpointsNumRetained = 1;

    /**
     * 是否开启增量ck，默认为false
     */
    private Boolean incrementalCheckpoints = false;

    /**
     * 状态后端是否从本地恢复，本地恢复目前只覆盖键控状态后端(包括EmbeddedRocksDBStateBackend和HashMapStateBackend)，默认为false
     */
    private Boolean localRecovery = false;

    /**
     * 构建ck配置
     *
     * @param dynamicFlinkConf flink动态配置
     */
    public void buildCheckpointProperties(Configuration dynamicFlinkConf) {
        dynamicFlinkConf.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, this.checkpointsNumRetained);
        dynamicFlinkConf.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, this.incrementalCheckpoints);
        dynamicFlinkConf.set(CheckpointingOptions.LOCAL_RECOVERY, this.localRecovery);
        ConfigValidateUtils.stringConfigValid(CheckpointingOptions.CHECKPOINT_STORAGE.key(), this.checkpointStorage,
                dynamicFlinkConf);
        ConfigValidateUtils.stringConfigValid(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(),
                this.checkpointDirectory,
                dynamicFlinkConf);
        ConfigValidateUtils.stringConfigValid(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), this.savepointDirectory,
                dynamicFlinkConf);
    }
}
