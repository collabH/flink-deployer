package com.flink.plugins.inf.config;

import com.flink.plugins.inf.utils.ConfigValidateUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.CheckpointingOptions;

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
@Builder
public class CheckpointConfig {
    /**
     * checkpoint存储路径
     */
    private String checkpointStorage;
    /**
     * savepoint存储目录
     */
    private String savepointDirectory;
    /**
     * checkpoint存储目录
     */
    private String checkpointDirectory;
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
     * @param dynamicProperties flink动态配置
     */
    public void buildCheckpointProperties(Properties dynamicProperties) {
        dynamicProperties.put(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key(), this.checkpointsNumRetained);
        dynamicProperties.put(CheckpointingOptions.INCREMENTAL_CHECKPOINTS.key(), this.incrementalCheckpoints);
        dynamicProperties.put(CheckpointingOptions.LOCAL_RECOVERY.key(), this.localRecovery);
        ConfigValidateUtils.stringConfigValid(CheckpointingOptions.CHECKPOINT_STORAGE.key(), this.checkpointStorage,
                dynamicProperties);
        ConfigValidateUtils.stringConfigValid(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(),
                this.checkpointDirectory,
                dynamicProperties);
        ConfigValidateUtils.stringConfigValid(CheckpointingOptions.SAVEPOINT_DIRECTORY.key(), this.savepointDirectory,
                dynamicProperties);
    }
}
