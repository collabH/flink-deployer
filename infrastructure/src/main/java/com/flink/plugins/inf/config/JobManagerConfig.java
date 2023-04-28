package com.flink.plugins.inf.config;

import com.flink.plugins.inf.utils.ConfigValidateUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;

import java.util.Properties;

/**
 * @fileName: JobManagerConfig.java
 * @description: jobManager配置
 * @author: huangshimin
 * @date: 2023/4/28 16:19
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class JobManagerConfig {
    /**
     * jobManager总内存设置，单位mb
     */
    private Long totalProcessMemoryMb;
    /**
     * jobManager flink框架内存，单位mb
     */
    private Long totalFlinkMemoryMb;
    /**
     * jobManager jvm heap区域内存，单位mb
     */
    private Long jvmHeapMemoryMb;
    /**
     * jobManager jvm off-heap区域内存，单位mb，默认128mb
     */
    private Long jvmOffHeapMemoryMb = 128L;

    /**
     * jobManager jvm-metaspace区域内存，单位mb，默认256mb
     */
    private Long jvmMetaspaceMemoryMb = 256L;
    /**
     * jvmOverhead大小配置，max、min/fraction只能选其一
     */
    private Long jvmOverheadMinMemoryMb;
    private Long jvmOverheadMaxMemoryMb;
    private Float jvmOverheadFraction;
    /**
     * 执行容错策略，默认为region，支持full、region
     */
    private String failoverStrategy = "region";

    /**
     * jobManager rpc端口
     */
    private Integer port = 6123;
    private Integer rpcBindPort;
    /**
     * jobManager rpc地址
     */
    private String address;
    /**
     * jobManager绑定的地址，默认为0.0.0.0
     */
    private String bindHost = "0.0.0.0";

    /**
     * 构建jobManager配置
     *
     * @param dynamicProperties flink动态配置
     */
    public void buildJobManagerProperties(Properties dynamicProperties) {
        ConfigValidateUtils.objConfigValid(JobManagerOptions.TOTAL_PROCESS_MEMORY.key(),
                MemorySize.ofMebiBytes(this.totalProcessMemoryMb), dynamicProperties);
        ConfigValidateUtils.objConfigValid(JobManagerOptions.TOTAL_FLINK_MEMORY.key(),
                MemorySize.ofMebiBytes(this.totalFlinkMemoryMb), dynamicProperties);
        // jvm
        ConfigValidateUtils.objConfigValid(JobManagerOptions.JVM_HEAP_MEMORY.key(),
                MemorySize.ofMebiBytes(this.jvmHeapMemoryMb), dynamicProperties);
        dynamicProperties.put(JobManagerOptions.OFF_HEAP_MEMORY.key(), this.jvmOffHeapMemoryMb);
        // metaspace
        dynamicProperties.put(JobManagerOptions.JVM_METASPACE.key(), this.jvmMetaspaceMemoryMb);
        // jvm-overhead
        ConfigValidateUtils.objConfigValid(JobManagerOptions.JVM_OVERHEAD_MAX.key(),
                MemorySize.ofMebiBytes(this.jvmOverheadMaxMemoryMb), dynamicProperties);
        ConfigValidateUtils.objConfigValid(JobManagerOptions.JVM_OVERHEAD_MIN.key(),
                MemorySize.ofMebiBytes(this.jvmOverheadMinMemoryMb), dynamicProperties);
        ConfigValidateUtils.objConfigValid(JobManagerOptions.JVM_OVERHEAD_FRACTION.key(),
                this.jvmOverheadFraction, dynamicProperties);
        // other
        dynamicProperties.put(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY.key(), this.failoverStrategy);
        dynamicProperties.put(JobManagerOptions.PORT.key(), this.port);
        ConfigValidateUtils.objConfigValid(JobManagerOptions.RPC_BIND_PORT.key(), this.rpcBindPort, dynamicProperties);
        ConfigValidateUtils.stringConfigValid(JobManagerOptions.ADDRESS.key(), this.address, dynamicProperties);
        dynamicProperties.put(JobManagerOptions.BIND_HOST.key(), this.bindHost);

    }
}
