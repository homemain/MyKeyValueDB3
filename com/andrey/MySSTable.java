package com.andrey;

import java.io.*;
import java.nio.file.*;
import java.util.Map;
import java.util.TreeMap;

public class MySSTable {
    private final String dataDir;
    private TreeMap<Long, Path> sortedFilesList;
    private volatile long fileCounter;

    public MySSTable(String dataDir) throws IOException {
        this.dataDir = dataDir;
        this.fileCounter = getNextFileCounter();
        Files.createDirectories(Paths.get(dataDir));
        this.sortedFilesList = new TreeMap<>((a, b) -> b.compareTo(a)); // Reverse order
        loadSortedFilesList();
    }
    
    public void flush(MyMemTable memTable) throws IOException {
        Map<String, String> data = memTable.getAll();
        if (data.isEmpty()) {
            return;
        }
        
        String filename = generateFilename();
        try (RandomAccessFile writer = new RandomAccessFile(filename, "rw")) {
            // Write number of entries at the start of file
            writer.writeInt(data.size());
            
            for (Map.Entry<String, String> entry : data.entrySet()) {
                String line = String.format("%s,%s\n", entry.getKey(), entry.getValue());
                writer.write(line.getBytes());
            }
        }
        loadSortedFilesList();           
    }

    private synchronized void loadSortedFilesList() throws IOException {
        sortedFilesList.clear();
        
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dataDir), 
                Constants.FILE_PREFIX + "*" + Constants.FILE_SUFFIX)) {
            for (Path file : stream) {
                String fileName = file.getFileName().toString();
                long fileNumber = Long.parseLong(fileName.substring(Constants.FILE_PREFIX.length(), 
                        fileName.length() - Constants.FILE_SUFFIX.length()));
                sortedFilesList.put(fileNumber, file);
            }
        }
    }
    
    private synchronized String generateFilename() {
        return Paths.get(dataDir, Constants.FILE_PREFIX + (fileCounter++) + Constants.FILE_SUFFIX).toString();
    }
    
    public void cleanup() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dataDir), Constants.FILE_PREFIX + "*" + Constants.FILE_SUFFIX)) {
            for (Path file : stream) {
                Files.delete(file);
            }
        }
    }

    public synchronized String get(String key) throws IOException {
        for (Path file : sortedFilesList.values()) {
            String result = binarySearchInFile(file.toFile(), key);
            if (result != null) {
                return Constants.TOMBSTONE.equals(result) ? null : result;
            }
        }
        return null;
    }

    private long getNextFileCounter() {
        try {
            long maxCounter = -1;
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dataDir), 
                    Constants.FILE_PREFIX + "*" + Constants.FILE_SUFFIX)) {
                for (Path file : stream) {
                    String fileName = file.getFileName().toString();
                    try {
                        long fileNumber = Long.parseLong(fileName.substring(Constants.FILE_PREFIX.length(), 
                            fileName.length() - Constants.FILE_SUFFIX.length()));
                        maxCounter = Math.max(maxCounter, fileNumber);
                    } catch (NumberFormatException e) {
                        // Skip files that don't match our naming pattern
                        continue;
                    }
                }
            }
            return maxCounter + 1;
        } catch (IOException e) {
            // If we can't read the directory, start from 0
            return 0;
        }
    }

    private String binarySearchInFile(File file, String searchKey) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            // Read number of entries
            int numEntries = raf.readInt();
            if (numEntries == 0) {
                return null;
            }

            // Perform binary search
            long start = raf.getFilePointer();
            long end = raf.length();
            
            while (start < end) {
                long mid = start + (end - start) / 2;
                
                // Seek to the start of a line
                raf.seek(mid);
                if (mid > start) {
                    // Skip partial line if we're not at the start
                    raf.readLine();
                }
                
                // Read and parse the line
                String line = raf.readLine();
                if (line == null) {
                    end = mid;
                    continue;
                }
                
                String[] parts = line.split(",", 2);
                if (parts.length != 2) {
                    continue;
                }
                
                int comparison = parts[0].compareTo(searchKey);
                if (comparison == 0) {
                    return parts[1];
                } else if (comparison < 0) {
                    start = raf.getFilePointer();
                } else {
                    end = mid;
                }
            }
        }
        return null;
    }

    public synchronized Map<String, String> getBatch(String keyStart, String keyEnd) throws IOException {
        Map<String, String> results = new TreeMap<>();
        
        // Iterate through files from newest to oldest
        for (Path file : sortedFilesList.values()) {
            Map<String, String> fileResults = readBatchFromFile(file.toFile(), keyStart, keyEnd);
            
            // Only add entries that we haven't seen yet
            for (Map.Entry<String, String> entry : fileResults.entrySet()) {
                if (!results.containsKey(entry.getKey())) {
                    results.put(entry.getKey(), entry.getValue());
                }
            }
        }
        
        return results;
    }

    private Map<String, String> readBatchFromFile(File file, String keyStart, String keyEnd) throws IOException {
        Map<String, String> results = new TreeMap<>();
        
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            // Skip the number of entries at the start
            int numEntries = raf.readInt();
            
            // Read through the file sequentially
            String line;
            while ((line = raf.readLine()) != null) {
                String[] parts = line.split(",", 2);
                if (parts.length != 2) continue;
                
                String key = parts[0];
                String value = parts[1];
                
                // Check if key is in range
                if (key.compareTo(keyStart) >= 0 && key.compareTo(keyEnd) <= 0) {
                    results.put(key, value);
                }
                
                // Optimization: if we've passed the end key, we can stop
                if (key.compareTo(keyEnd) > 0) {
                    break;
                }
            }
        }
        
        return results;
    }
} 