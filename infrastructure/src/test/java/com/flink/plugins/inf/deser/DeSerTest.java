package com.flink.plugins.inf.deser;

import com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @fileName: DeSerTest.java
 * @description: DeSerTest.java类说明
 * @author: huangshimin
 * @date: 2023/4/6 17:11
 */
public class DeSerTest {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        deSerObj();
    }

    public static <T> void deSerObj() throws IOException, ClassNotFoundException {
        Config<Item> config = new Config<>();
        config.setName("test-items");
        Item item = new Item();
        item.setAge("1");
        item.setName("hh");
        config.setSer(Lists.newArrayList(item));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(config);
        oos.flush();
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        Config<T> deSerObj = (Config<T>) ois.readObject();
        System.out.println(deSerObj);
    }
}
