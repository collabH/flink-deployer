package com.flink.plugins.inf.constants;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @fileName: DeployerTypeEnums.java
 * @description: 部署器类型
 * @author: huangshimin
 * @date: 2023/5/8 15:52
 */
@AllArgsConstructor
@NoArgsConstructor
public enum DeployerTypeEnums {
    YARN("yarn"),
    KUBERNETES("kubernetes");

    @Getter
    private String type;
}
