package com.andrey;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class MyWriteAheadLog {
    private final String logFile;
    private volatile BufferedWriter writer;
    private final ReentrantLock writeLock = new ReentrantLock();
    
    public MyWriteAheadLog(String filename) throws IOException {
        this.logFile = filename;
        // Create parent directories if they don't exist
        Files.createDirectories(Paths.get(filename).getParent());
        // Open writer in append mode
        this.writer = new BufferedWriter(new FileWriter(filename, true));
    }
    
    public void logPut(String key, String value) throws IOException {
        writeLock.lock();
        try {
            String entry = String.format("PUT,%s,%s\n", key, value);
            writer.write(entry);
            writer.flush();
        } finally {
            writeLock.unlock();
        }
    }
    
    public void logDelete(String key) throws IOException {
        writeLock.lock();
        try {
            String entry = String.format("DELETE,%s\n", key);
            writer.write(entry);
            writer.flush();
        } finally {
            writeLock.unlock();
        }
    }
    
    public List<LogEntry> recover() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        writeLock.lock();
        try {
            try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        String operation = parts[0];
                        String key = parts[1];
                        if ("PUT".equals(operation) && parts.length >= 3) {
                            entries.add(new LogEntry(Operation.PUT, key, parts[2]));
                        } else if ("DELETE".equals(operation)) {
                            entries.add(new LogEntry(Operation.DELETE, key, null));
                        }
                    }
                }
            }
            return entries;
        } finally {
            writeLock.unlock();
        }
    }
    
    public void gracefulClose() throws IOException {
        writeLock.lock();
        try {
            writer.close();
        } finally {
            writeLock.unlock();
        }
    }
    
    public void cleanup() throws IOException {
        writeLock.lock();
        try {
            // Close current writer
            writer.close();
            // Create new empty file
            new FileWriter(logFile, false).close();
            // Reopen writer in append mode
            writer = new BufferedWriter(new FileWriter(logFile, true));
        } finally {
            writeLock.unlock();
        }
    }
    
    public enum Operation {
        PUT,
        DELETE
    }
    
    public static class LogEntry {
        private final Operation operation;
        private final String key;
        private final String value;
        
        public LogEntry(Operation operation, String key, String value) {
            this.operation = operation;
            this.key = key;
            this.value = value;
        }
        
        public Operation getOperation() {
            return operation;
        }
        
        public String getKey() {
            return key;
        }
        
        public String getValue() {
            return value;
        }
    }
} 