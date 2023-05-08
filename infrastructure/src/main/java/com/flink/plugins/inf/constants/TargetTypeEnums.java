package com.flink.plugins.inf.constants;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @fileName: TargetTypeEnums.java
 * @description: flink部署类型
 * @author: huangshimin
 * @date: 2023/5/8 15:03
 */
@AllArgsConstructor
@NoArgsConstructor
public enum TargetTypeEnums {
    REMOTE("remote"),
    LOCAL("local"),
    YARN_PER_JOB("yarn-per-job"),
    YARN_SESSION("yarn-session"),
    KUBERNETES_SESSION("kubernetes-session"),
    YARN_APPLICATION("yarn-application"),
    KUBERNETES_APPLICATION("kubernetes-application");
    @Getter
    private String target;
}
