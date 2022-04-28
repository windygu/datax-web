package com.wugui.datax.rpc.registry.impl;

import com.wugui.datax.rpc.registry.ServiceRegistry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * LocalServiceRegistry 继承ServiceRegistry 实现所有抽象方法。将服务注册到本地（项目内）。
 * *              - key01(datax-executor)
 *  *                  - value01 (ip:port01)
 *  *                  - value02 (ip:port02)
 * @author xuxueli 2018-10-17
 */
public class LocalServiceRegistry extends ServiceRegistry {

    /**
     * 就一个registryData 成员。使用tree set和一致性哈希有关（待补充）
     */
    private Map<String, TreeSet<String>> registryData;


    /**
     * @param param ignore, not use
     */
    @Override
    public void start(Map<String, String> param) {
        registryData = new HashMap<>();
    }

    @Override
    public void stop() {
        registryData.clear();
    }

    //往registryData中增量注册。有一点不明白的就是为啥key是多个？
    //应该是register而不是registry，注册服务用的
    @Override
    public boolean registry(Set<String> keys, String value) {
        if (keys == null || keys.size() == 0 || value == null || value.trim().length() == 0) {
            return false;
        }
        for (String key : keys) {
            TreeSet<String> values = registryData.get(key);
            if (values == null) {
                values = new TreeSet<>();
                registryData.put(key, values);
            }
            values.add(value);
        }
        return true;
    }

    @Override
    public boolean remove(Set<String> keys, String value) {
        if (keys == null || keys.size() == 0 || value == null || value.trim().length() == 0) {
            return false;
        }
        for (String key : keys) {
            TreeSet<String> values = registryData.get(key);
            if (values != null) {
                values.remove(value);
            }
        }
        return true;
    }

    //有两个重载方法 一个是获取多个key的一个是获取一个key的
    @Override
    public Map<String, TreeSet<String>> discovery(Set<String> keys) {
        if (keys == null || keys.size() == 0) {
            return null;
        }
        Map<String, TreeSet<String>> registryDataTmp = new HashMap<String, TreeSet<String>>();
        for (String key : keys) {
            TreeSet<String> valueSetTmp = discovery(key);
            if (valueSetTmp != null) {
                registryDataTmp.put(key, valueSetTmp);
            }
        }
        return registryDataTmp;
    }

    @Override
    public TreeSet<String> discovery(String key) {
        return registryData.get(key);
    }

}
