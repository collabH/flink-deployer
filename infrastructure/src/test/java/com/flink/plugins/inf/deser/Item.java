package com.flink.plugins.inf.deser;

import lombok.Data;

import java.io.Serializable;

/**
 * @fileName: Item.java
 * @description: Item.java类说明
 * @author: huangshimin
 * @date: 2023/4/6 17:11
 */
@Data
public class Item implements Serializable {
    private String name;
    private String age;
}
