package com.flink.plugins.inf.constants;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @fileName: KubernetesUseCaseEnums.java
 * @description: k8s use case枚举
 * @author: huangshimin
 * @date: 2023/5/9 10:47
 */
@AllArgsConstructor
@NoArgsConstructor
public enum KubernetesUseCaseEnums {
    CLIENT("client"),
    RESOURCEMANAGER("resourcemanager"),
    KUBERNETES_HA_SERVICES("kubernetes-ha-services");

    @Getter
    private String useCase;
}
