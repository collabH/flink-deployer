package com.flink.plugins.inf.config;

import com.flink.plugins.inf.utils.ConfigValidateUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
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
@Builder
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
    private Long networkMemoryFraction;
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
     * @param dynamicProperties flink动态配置
     */
    public void buildTaskManagerProperties(Properties dynamicProperties) {
        dynamicProperties.put(TaskManagerOptions.NUM_TASK_SLOTS.key(), this.numTaskSlot);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.CPU_CORES.key(), this.cpuCores,
                dynamicProperties);
        // process、flink
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                MemorySize.ofMebiBytes(this.totalProcessMemoryMb), dynamicProperties);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.TOTAL_FLINK_MEMORY.key(),
                MemorySize.ofMebiBytes(this.totalFlinkMemoryMb), dynamicProperties);
        // framework
        dynamicProperties.put(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(),
                MemorySize.ofMebiBytes(this.frameworkHeapMemoryMb));
        dynamicProperties.put(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key(),
                MemorySize.ofMebiBytes(this.frameworkOffHeapMemoryMb));
        // task
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.TASK_HEAP_MEMORY.key(),
                MemorySize.ofMebiBytes(this.taskHeapMemoryMb), dynamicProperties);
        dynamicProperties.put(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(),
                MemorySize.ofMebiBytes(this.taskOffHeapMemoryMb));
        // managed memory
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
                MemorySize.ofMebiBytes(this.managedMemorySizeMb), dynamicProperties);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(),
                this.managedMemoryFraction, dynamicProperties);
        // metaspace
        dynamicProperties.put(TaskManagerOptions.JVM_METASPACE.key(),
                MemorySize.ofMebiBytes(this.jvmMetaspaceMemoryMb));
        // network
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.NETWORK_MEMORY_MAX.key(),
                MemorySize.ofMebiBytes(this.networkMaxMemoryMb), dynamicProperties);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                MemorySize.ofMebiBytes(this.networkMinMemoryMb), dynamicProperties);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.NETWORK_MEMORY_FRACTION.key(),
                this.networkMemoryFraction, dynamicProperties);
        // jvm-overhead
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.JVM_OVERHEAD_MAX.key(),
                MemorySize.ofMebiBytes(this.jvmOverheadMaxMemoryMb), dynamicProperties);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.JVM_OVERHEAD_MIN.key(),
                MemorySize.ofMebiBytes(this.jvmOverheadMinMemoryMb), dynamicProperties);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.JVM_OVERHEAD_FRACTION.key(),
                this.jvmOverheadFraction, dynamicProperties);
        // host
        ConfigValidateUtils.stringConfigValid(TaskManagerOptions.HOST.key(), this.host, dynamicProperties);
        dynamicProperties.put(TaskManagerOptions.BIND_HOST.key(), this.bindHost);
        dynamicProperties.put(TaskManagerOptions.RPC_PORT.key(), this.rpcPort);
        ConfigValidateUtils.objConfigValid(TaskManagerOptions.RPC_BIND_PORT.key(), this.rpcBindPort, dynamicProperties);
        dynamicProperties.put(TaskManagerOptions.REGISTRATION_TIMEOUT.key(),
                Duration.ofSeconds(this.registrationTimeoutSecond));
        dynamicProperties.put(TaskManagerOptions.SLOT_TIMEOUT.key(),
                Duration.ofSeconds(this.slotTimeoutSecond));

    }
}
