package net.steveperkins.fitnessjiffy.etl.util;

import java.util.HashMap;

public class NoNullsMap<K, V> extends HashMap<K, V> {

    @Override
    public V put(K key, V value) {
        return (key != null && value != null) ? super.put(key, value) : null;
    }

    @Override
    public void putAll(java.util.Map<? extends K,? extends V> map) {
        for(K key : map.keySet()) {
            put(key, map.get(key));
        }
    }

}
