package com.andrey;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;

public class MyMemTable {
    private final ConcurrentSkipListMap<String, String> data;
    private final AtomicInteger recordCount;
    private final int recordLimit;
    
    public MyMemTable(int recordLimit) {
        this.data = new ConcurrentSkipListMap<>();
        this.recordCount = new AtomicInteger(0);
        this.recordLimit = recordLimit;
    }
    
    public void put(String key, String value) {
        String oldValue = data.put(key, value);
        if (oldValue == null) {
            // Only increment if this is a new key
            recordCount.incrementAndGet();
        }
    }

    
    public String get(String key) {
        String value = data.get(key);
        return Constants.TOMBSTONE.equals(value) ? null : value;
    }
    
    public void delete(String key) {
        String oldValue = data.put(key, Constants.TOMBSTONE);
        if (oldValue == null) {
            // Only increment if this is a new key
            recordCount.incrementAndGet();
        }
    }
    
    public boolean isFull() {
        return recordCount.get() >= recordLimit;
    }
    
    public Map<String, String> getAll() {
        return data;
    }
    
    public void clear() {
        data.clear();
        recordCount.set(0);
    }
    
    public int getCurrentSize() {
        return recordCount.get();
    }
    
    public boolean isEmpty() {
        return recordCount.get() == 0;
    }

    public Map<String, String> getBatch(String keyStart, String keyEnd) {
        return data.subMap(keyStart, true, keyEnd, true);
    }
} 