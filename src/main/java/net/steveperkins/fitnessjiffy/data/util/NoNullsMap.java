package net.steveperkins.fitnessjiffy.data.util;

import com.google.common.collect.ForwardingMap;

import java.util.HashMap;
import java.util.Map;

public class NoNullsMap<K, V> extends ForwardingMap<K, V> {

    final Map<K, V> delegate = new HashMap<K, V>();

    @Override
    protected Map<K, V> delegate() {
        return delegate;
    }

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
