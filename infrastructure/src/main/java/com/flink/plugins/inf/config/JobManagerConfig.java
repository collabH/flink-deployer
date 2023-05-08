package com.flink.plugins.inf.config;

import com.flink.plugins.inf.utils.ConfigValidateUtils;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
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
     * @param dynamicFlinkConf flink动态配置
     */
    public void buildJobManagerProperties(Configuration dynamicFlinkConf) {
        ConfigValidateUtils.memorySizeValid(JobManagerOptions.TOTAL_PROCESS_MEMORY,
                this.totalProcessMemoryMb, dynamicFlinkConf);
        ConfigValidateUtils.memorySizeValid(JobManagerOptions.TOTAL_FLINK_MEMORY,
                this.totalFlinkMemoryMb, dynamicFlinkConf);
        // jvm
        ConfigValidateUtils.memorySizeValid(JobManagerOptions.JVM_HEAP_MEMORY,
                this.jvmHeapMemoryMb, dynamicFlinkConf);
        dynamicFlinkConf.set(JobManagerOptions.OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(this.jvmOffHeapMemoryMb));
        // metaspace
        dynamicFlinkConf.set(JobManagerOptions.JVM_METASPACE, MemorySize.ofMebiBytes(this.jvmMetaspaceMemoryMb));
        // jvm-overhead
        ConfigValidateUtils.memorySizeValid(JobManagerOptions.JVM_OVERHEAD_MAX,
                this.jvmOverheadMaxMemoryMb, dynamicFlinkConf);
        ConfigValidateUtils.memorySizeValid(JobManagerOptions.JVM_OVERHEAD_MIN,
                this.jvmOverheadMinMemoryMb, dynamicFlinkConf);
        ConfigValidateUtils.objConfigValid(JobManagerOptions.JVM_OVERHEAD_FRACTION,
                this.jvmOverheadFraction, dynamicFlinkConf);
        // other
        dynamicFlinkConf.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, this.failoverStrategy);
        dynamicFlinkConf.set(JobManagerOptions.PORT, this.port);
        ConfigValidateUtils.objConfigValid(JobManagerOptions.RPC_BIND_PORT, this.rpcBindPort, dynamicFlinkConf);
        ConfigValidateUtils.stringConfigValid(JobManagerOptions.ADDRESS.key(), this.address, dynamicFlinkConf);
        dynamicFlinkConf.set(JobManagerOptions.BIND_HOST, this.bindHost);

    }
}
