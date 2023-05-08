package com.flink.plugins.inf.config;

import com.flink.plugins.inf.utils.ConfigValidateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;

import java.time.Duration;
import java.util.Properties;

/**
 * @fileName: TaskManagerConfig.java
 * @description: taskManager配置
 * @author: huangshimin
 * @date: 2023/4/28 16:43
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TaskManagerConfig {
    /**
     * 每个taskmanager slot个数，默认为1
     */
    private Integer numTaskSlot = 1;
    /**
     * taskmanager cpu cores数
     */
    private Double cpuCores;
    /**
     * taskmanager process memory单位mb
     */
    private Long totalProcessMemoryMb;
    /**
     * taskmanager flink memory单位mb
     */
    private Long totalFlinkMemoryMb;
    /**
     * taskmanager framework heap size单位mb
     */
    private Long frameworkHeapMemoryMb = 128L;
    /**
     * taskmanager framework off-heap size单位mb
     */
    private Long frameworkOffHeapMemoryMb = 128L;
    /**
     * taskmanager task heap size单位mb
     */
    private Long taskHeapMemoryMb;
    /**
     * taskmanager task off-heap size单位mb
     */
    private Long taskOffHeapMemoryMb = 0L;
    /**
     * taskmanager managed memory size单位mb
     */
    private Long managedMemorySizeMb;
    /**
     * taskmanager managed memory fraction
     */
    private Float managedMemoryFraction;
    /**
     * taskmanager jvm-metaspace区域内存，单位mb，默认256mb
     */
    private Long jvmMetaspaceMemoryMb = 256L;
    /**
     * taskmanager network memory config,max、min/fraction只能选其一
     */
    private Long networkMinMemoryMb;
    private Long networkMaxMemoryMb;
    private Float networkMemoryFraction;
    /**
     * taskManager jvmOverhead大小配置，max、min/fraction只能选其一
     */
    private Long jvmOverheadMinMemoryMb;
    private Long jvmOverheadMaxMemoryMb;
    private Float jvmOverheadFraction;

    /**
     * taskmanager host配置
     */
    private String host;
    /**
     * taskmanager绑定的host，默认为0.0.0.0
     */
    private String bindHost = "0.0.0.0";
    /**
     * taskmanager rpc port，默认为0
     */
    private String rpcPort = "0";

    /**
     * taskmanager rpc bind port
     */
    private Integer rpcBindPort;

    /**
     * taskmanager register超时时间，默认5分钟 单位为秒
     */
    private Long registrationTimeoutSecond = 5 * 60 * 1000L;

    /**
     * slot超时时间，默认10s，单位秒
     */
    private Integer slotTimeoutSecond = 10;

    /**
     * 构建taskManager配置
     *
     * @param dynamicFlinkConf flink动态配置
     */
    public void buildTaskManagerProperties(Configuration dynamicFlinkConf) {
        dynamicFlinkConf.set(TaskManagerOptions.NUM_TASK_SLOTS, this.numTaskSlot);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.CPU_CORES, this.cpuCores,
                dynamicFlinkConf);
        // process、flink
        ConfigValidateUtils.memorySizeValid(TaskManagerOptions.TOTAL_PROCESS_MEMORY,
                this.totalProcessMemoryMb, dynamicFlinkConf);
        ConfigValidateUtils.memorySizeValid(TaskManagerOptions.TOTAL_FLINK_MEMORY,
                this.totalFlinkMemoryMb, dynamicFlinkConf);
        // framework
        dynamicFlinkConf.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY,
                MemorySize.ofMebiBytes(this.frameworkHeapMemoryMb));
        dynamicFlinkConf.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY,
                MemorySize.ofMebiBytes(this.frameworkOffHeapMemoryMb));
        // task
        ConfigValidateUtils.memorySizeValid(TaskManagerOptions.TASK_HEAP_MEMORY,
                this.taskHeapMemoryMb, dynamicFlinkConf);
        dynamicFlinkConf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY,
                MemorySize.ofMebiBytes(this.taskOffHeapMemoryMb));
        // managed memory
        ConfigValidateUtils.memorySizeValid(TaskManagerOptions.MANAGED_MEMORY_SIZE,
                this.managedMemorySizeMb, dynamicFlinkConf);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.MANAGED_MEMORY_FRACTION,
                this.managedMemoryFraction, dynamicFlinkConf);
        // metaspace
        dynamicFlinkConf.set(TaskManagerOptions.JVM_METASPACE,
                MemorySize.ofMebiBytes(this.jvmMetaspaceMemoryMb));
        // network
        ConfigValidateUtils.memorySizeValid(TaskManagerOptions.NETWORK_MEMORY_MAX,
                this.networkMaxMemoryMb, dynamicFlinkConf);
        ConfigValidateUtils.memorySizeValid(TaskManagerOptions.NETWORK_MEMORY_MIN,
                this.networkMinMemoryMb, dynamicFlinkConf);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.NETWORK_MEMORY_FRACTION,
                this.networkMemoryFraction, dynamicFlinkConf);
        // jvm-overhead
        ConfigValidateUtils.memorySizeValid(TaskManagerOptions.JVM_OVERHEAD_MAX,
                this.jvmOverheadMaxMemoryMb, dynamicFlinkConf);
        ConfigValidateUtils.memorySizeValid(TaskManagerOptions.JVM_OVERHEAD_MIN,
                this.jvmOverheadMinMemoryMb, dynamicFlinkConf);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.JVM_OVERHEAD_FRACTION,
                this.jvmOverheadFraction, dynamicFlinkConf);
        // host
        ConfigValidateUtils.stringConfigValid(TaskManagerOptions.HOST.key(), this.host, dynamicFlinkConf);
        dynamicFlinkConf.set(TaskManagerOptions.BIND_HOST, this.bindHost);
        dynamicFlinkConf.set(TaskManagerOptions.RPC_PORT, this.rpcPort);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.RPC_BIND_PORT, this.rpcBindPort, dynamicFlinkConf);
        dynamicFlinkConf.set(TaskManagerOptions.REGISTRATION_TIMEOUT,
                Duration.ofSeconds(this.registrationTimeoutSecond));
        dynamicFlinkConf.set(TaskManagerOptions.SLOT_TIMEOUT,
                Duration.ofSeconds(this.slotTimeoutSecond));

    }
}
