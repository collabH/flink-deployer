package com.flink.plugins.inf.deser;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * @fileName: Config.java
 * @description: Config.java类说明
 * @author: huangshimin
 * @date: 2023/4/6 17:11
 */
@Data
public class Config<T> implements Serializable {
    private String name;
    private List<T> ser;
}
