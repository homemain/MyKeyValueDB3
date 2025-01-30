package com.andrey;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MyStorageEngine {
    private final MyMemTable memTable;
    private final MyWriteAheadLog wal;
    private final MySSTable ssTable;
    
    public MyStorageEngine() {
        try {
            memTable = new MyMemTable(Constants.DEFAULT_RECORD_LIMIT);
            wal = new MyWriteAheadLog(Constants.WAL_PATH);
            ssTable = new MySSTable(Constants.DATA_DIR);
            recoverFromWal();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize storage", e);
        }
    }
    
    private void recoverFromWal() throws IOException {
        
        List<MyWriteAheadLog.LogEntry> entries = wal.recover();
        for (MyWriteAheadLog.LogEntry entry : entries) {
            switch (entry.getOperation()) {
                case PUT:
                    memTable.put(entry.getKey(), entry.getValue());
                    break;
                case DELETE:
                    memTable.delete(entry.getKey());
                    break;
            }
        }
    }
    
    public synchronized void put(String key, String value) {
        try {
            wal.logPut(key, value);
            memTable.put(key, value);
            
            if (memTable.isFull()) {
                flushMemTable();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to put key-value pair", e);
        }
    }
    
    public String get(String key) {
        // First check MemTable for most recent data
        String value = memTable.get(key);
        
        // If not found in MemTable, try to find in SSTable
        if (value == null) {
            try {
                value = ssTable.get(key);
                if (value != null) {
                    memTable.put(key, value);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read from SSTable", e);
            }
        }
        return value;
    }
    
    public synchronized void delete(String key) {
        try {
            wal.logDelete(key);
            memTable.delete(key);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete key", e);
        }
    }
    
    private synchronized void flushMemTable() throws IOException {
        ssTable.flush(memTable);
        memTable.clear();
        wal.cleanup();
    }
    
    public void gracefulClose() {
        try {
            flushMemTable();
            wal.gracefulClose();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close storage", e);
        }
    }

    public void cleanup() {
        try {
            // Clean up SSTable files
            ssTable.cleanup();
            // Clean up WAL file
            wal.cleanup();
        } catch (IOException e) {
            throw new RuntimeException("Failed to cleanup storage", e);
        }
    }

    public synchronized Map<String, String> getBatch(String keyStart, String keyEnd) {
        Map<String, String> results = new TreeMap<>();
        
        // First get from memTable
        Map<String, String> memTableResults = memTable.getBatch(keyStart, keyEnd);
        results.putAll(memTableResults);
        
        try {
            // Then get from SSTable
            Map<String, String> ssTableResults = ssTable.getBatch(keyStart, keyEnd);
            
            // Merge results, giving preference to memTable values
            for (Map.Entry<String, String> entry : ssTableResults.entrySet()) {
                if (!results.containsKey(entry.getKey())) {
                    results.put(entry.getKey(), entry.getValue());
                }
            }
            
            // Filter out TOMBSTONE markers
            results.entrySet().removeIf(entry -> "TOMBSTONE".equals(entry.getValue()));
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to read batch from SSTable", e);
        }
        
        return results;
    }

    public synchronized void putBatch(Map<String, String> entries) {
        try {
            // First log all entries to WAL
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                wal.logPut(entry.getKey(), entry.getValue());
            }
            
            // Then put all entries in memTable
            for (Map.Entry<String, String> entry : entries.entrySet()) {
                memTable.put(entry.getKey(), entry.getValue());
                
                // Check if memTable is full after each put
                if (memTable.isFull()) {
                    flushMemTable();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to put batch", e);
        }
    }

} 
