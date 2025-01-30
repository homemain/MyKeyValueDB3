package com.andrey;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class MyTestCases {
    private static final String GREEN = "\u001B[32m";
    private static final String RED = "\u001B[31m";
    private static final String RESET = "\u001B[0m";
    
    private static void cleanupDataDir() {
        try {
            // Delete WAL file
            Files.deleteIfExists(Paths.get("data/wal.log"));
            
            // Delete all SSTable files
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get("data/sstables"), "sstable-*.db")) {
                for (Path file : stream) {
                    Files.delete(file);
                }
            }
            
            // Delete directories if empty
            Files.deleteIfExists(Paths.get("data/sstables"));
            Files.deleteIfExists(Paths.get("data"));
            
        } catch (IOException e) {
            System.err.println("Failed to cleanup: " + e.getMessage());
        }
    }
    
    private static void assertTest(String testName, boolean condition) {
        if (condition) {
            System.out.println(GREEN + "✓ " + testName + " passed" + RESET);
        } else {
            System.out.println(RED + "✗ " + testName + " failed" + RESET);
            throw new AssertionError("Test failed: " + testName);
        }
    }
    
    private static void testBasicOperations() {
        System.out.println("\n=== Testing Basic Operations ===");
        MyStorageEngine storage = new MyStorageEngine();
        
        try {
            // Test put and get
            storage.put("key1", "value1");
            assertTest("Basic put/get", "value1".equals(storage.get("key1")));
            
            // Test update
            storage.put("key1", "value2");
            assertTest("Update existing key", "value2".equals(storage.get("key1")));
            
            // Test delete
            storage.delete("key1");
            assertTest("Delete key", storage.get("key1") == null);
            
            // Test non-existent key
            assertTest("Get non-existent key", storage.get("nonexistent") == null);
            
        } finally {
            storage.gracefulClose();
            storage.cleanup();
        }
    }
    
    private static void testMemTableFlush() {
        System.out.println("\n=== Testing MemTable Flush ===");
        MyStorageEngine storage = new MyStorageEngine();
        
        try {
            // Put enough entries to trigger multiple flushes
            for (int i = 0; i < 250; i++) {
                storage.put("key" + i, "value" + i);
            }
            
            // Verify all values are still accessible
            boolean allValuesAccessible = true;
            for (int i = 0; i < 250; i++) {
                String value = storage.get("key" + i);
                if (!("value" + i).equals(value)) {
                    allValuesAccessible = false;
                    break;
                }
            }
            
            assertTest("All values accessible after flush", allValuesAccessible);
        } finally {
            storage.gracefulClose();
            storage.cleanup();
        }
    }
    
    private static void testPersistence() {
        System.out.println("\n=== Testing Persistence ===");
        
        MyStorageEngine storage1 = new MyStorageEngine();
        try {
            storage1.put("persist1", "value1");
            storage1.put("persist2", "value2");
            storage1.gracefulClose();
            
            MyStorageEngine storage2 = new MyStorageEngine();
            try {
                assertTest("Recover value1", "value1".equals(storage2.get("persist1")));
                assertTest("Recover value2", "value2".equals(storage2.get("persist2")));
            } finally {
                storage2.gracefulClose();
                storage2.cleanup();
            }
        } finally {
            storage1.cleanup();
        }
    }
    
    private static void testConcurrentAccess() throws InterruptedException {
        System.out.println("\n=== Testing Concurrent Access ===");
        MyStorageEngine storage = new MyStorageEngine();
        
        try {
            int numThreads = 20;
            int operationsPerThread = 100;
            CountDownLatch latch = new CountDownLatch(numThreads);
            ConcurrentHashMap<String, String> verificationMap = new ConcurrentHashMap<>();
            ConcurrentLinkedQueue<Throwable> exceptions = new ConcurrentLinkedQueue<>();
            
            Thread[] threads = new Thread[numThreads];
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                threads[i] = new Thread(() -> {
                    try {
                        for (int j = 0; j < operationsPerThread; j++) {
                            String key = "key-" + threadId + "-" + j;
                            String value = "value-" + threadId + "-" + j;
                            storage.put(key, value);
                            verificationMap.put(key, value);
                        }
                    } catch (Throwable e) {
                        exceptions.add(e);
                        System.err.println("Thread " + threadId + " failed: " + e.getMessage());
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
                threads[i].start();
            }
            
            latch.await();
            
            // Check if any threads failed
            if (!exceptions.isEmpty()) {
                Throwable firstException = exceptions.peek();
                throw new RuntimeException("One or more threads failed", firstException);
            }
            
            // Verify all values
            boolean allCorrect = true;
            String failedKey = null;
            String expectedValue = null;
            String actualValue = null;
            
            for (Map.Entry<String, String> entry : verificationMap.entrySet()) {
                String value = storage.get(entry.getKey());
                if (!entry.getValue().equals(value)) {
                    allCorrect = false;
                    failedKey = entry.getKey();
                    expectedValue = entry.getValue();
                    actualValue = value;
                    break;
                }
            }
            
            if (!allCorrect) {
                throw new AssertionError(String.format(
                    "Value mismatch for key '%s': expected '%s', got '%s'",
                    failedKey, expectedValue, actualValue));
            }
            
            assertTest("Concurrent operations", true);
        } finally {
            //storage.gracefulClose();
            //storage.cleanup();
        }
    }
    
    private static void testHighLoad() {
        System.out.println("\n=== Testing High Load ===");
        MyStorageEngine storage = new MyStorageEngine();
        
        try {
            int numOperations = 1000;
            
            long startTime = System.currentTimeMillis();
            
            // Write test
            System.out.println("Writing " + numOperations + " records...");
            for (int i = 0; i < numOperations; i++) {
                storage.put("loadkey" + i, "loadvalue" + i);
                if (i % 100 == 0) {
                    System.out.println("Written " + i + " records");
                }
            }
            
            long writeTime = System.currentTimeMillis() - startTime;
            System.out.println("Write time: " + writeTime + "ms");
            System.out.println("Write throughput: " + (numOperations * 1000.0 / writeTime) + " ops/sec");
            
            // Sequential read test
            startTime = System.currentTimeMillis();
            System.out.println("\nSequential reading " + numOperations + " records...(worst case scenario)");
            boolean allReadsSuccessful = true;
            for (int i = 0; i < numOperations; i++) {
                String value = storage.get("loadkey" + i);
                if (!("loadvalue" + i).equals(value)) {
                    allReadsSuccessful = false;
                    break;
                }
                if (i % 100 == 0) {
                    System.out.println("Read " + i + " records");
                }
            }
            
            long seqReadTime = System.currentTimeMillis() - startTime;
            System.out.println("Sequential read time: " + seqReadTime + "ms");
            System.out.println("Sequential read throughput: " + (numOperations * 1000.0 / seqReadTime) + " ops/sec");
            
            // Random read test
            startTime = System.currentTimeMillis();
            System.out.println("\nRandom reading " + numOperations + " records...");
            Random random = new Random();
            int successfulRandomReads = 0;
            int totalRandomReads = numOperations;
            
            for (int i = 0; i < totalRandomReads; i++) {
                int randomIndex = random.nextInt(numOperations);
                String value = storage.get("loadkey" + randomIndex);
                if (("loadvalue" + randomIndex).equals(value)) {
                    successfulRandomReads++;
                }
                if (i % 100 == 0) {
                    System.out.println("Random read " + i + " records");
                }
            }
            
            long randomReadTime = System.currentTimeMillis() - startTime;
            System.out.println("Random read time: " + randomReadTime + "ms");
            System.out.println("Random read throughput: " + (totalRandomReads * 1000.0 / randomReadTime) + " ops/sec");
            System.out.println("Random read success rate: " + (successfulRandomReads * 100.0 / totalRandomReads) + "%");
            
            assertTest("Sequential read operations", allReadsSuccessful);
            assertTest("Random read operations", successfulRandomReads == totalRandomReads);
        } finally {
            storage.gracefulClose();
            storage.cleanup();
        }
    }
    
    private static void testBatchOperations() {
        System.out.println("\n=== Testing Batch Operations ===");
        MyStorageEngine storage = new MyStorageEngine();
        
        try {
            // Part 1: Test putBatch
            Map<String, String> batchData = new TreeMap<>();
            batchData.put("batch1", "value1");
            batchData.put("batch2", "value2");
            batchData.put("batch3", "value3");
            
            storage.putBatch(batchData);
            
            // Verify batch put
            assertTest("Batch put verification", 
                "value1".equals(storage.get("batch1")) &&
                "value2".equals(storage.get("batch2")) &&
                "value3".equals(storage.get("batch3")));
            
            // Test batch put with existing keys
            Map<String, String> updateBatch = new TreeMap<>();
            updateBatch.put("batch1", "newvalue1");
            updateBatch.put("batch4", "value4");
            
            storage.putBatch(updateBatch);
            
            // Verify updates
            assertTest("Batch put update verification",
                "newvalue1".equals(storage.get("batch1")) &&
                "value4".equals(storage.get("batch4")));
            
            // Part 2: Test getBatch
            // Prepare test data with known ordering
            storage.put("key-a1", "value1");
            storage.put("key-a2", "value2");
            storage.put("key-b1", "value3");
            storage.put("key-b2", "value4");
            storage.put("key-c1", "value5");
            storage.put("key-c2", "value6");
            
            // Test exact range
            Map<String, String> result1 = storage.getBatch("key-b1", "key-b2");
            assertTest("Exact range batch get", 
                result1.size() == 2 && 
                "value3".equals(result1.get("key-b1")) && 
                "value4".equals(result1.get("key-b2")));
            
            // Test partial range
            Map<String, String> result2 = storage.getBatch("key-a2", "key-b1");
            assertTest("Partial range batch get", 
                result2.size() == 2 && 
                "value2".equals(result2.get("key-a2")) && 
                "value3".equals(result2.get("key-b1")));
            
            // Test range with deleted item
            storage.delete("key-b1");
            Map<String, String> result3 = storage.getBatch("key-b1", "key-b2");
            assertTest("Range with deleted item", 
                result3.size() == 1 && 
                "value4".equals(result3.get("key-b2")));
            
            // Test range across multiple SSTable files
            // Force a flush by adding more entries
            Map<String, String> largeBatch = new TreeMap<>();
            for (int i = 0; i < 15; i++) {
                largeBatch.put("key-x" + i, "value" + i);
            }
            storage.putBatch(largeBatch);
            
            // Add more entries after flush
            storage.put("key-y1", "valueY1");
            storage.put("key-y2", "valueY2");
            
            Map<String, String> result4 = storage.getBatch("key-x5", "key-y1");
            assertTest("Range across SSTable files", 
                result4.containsKey("key-x5") && 
                result4.containsKey("key-y1") &&
                "value5".equals(result4.get("key-x5")) &&
                "valueY1".equals(result4.get("key-y1")));
            
            // Test empty range
            Map<String, String> result5 = storage.getBatch("key-m1", "key-m2");
            assertTest("Empty range batch get", result5.isEmpty());
            
        } finally {
            storage.gracefulClose();
            storage.cleanup();
        }
    }
    
    public static void main(String[] args) {
        try {
            System.out.println("Starting test cases...");
            cleanupDataDir();
            
            testBasicOperations();
            testMemTableFlush();
            testPersistence();
            testHighLoad();
            testConcurrentAccess();
            testBatchOperations();
            
            System.out.println(GREEN + "\nAll tests passed successfully!" + RESET);
            
        } catch (AssertionError | InterruptedException e) {
            System.out.println(RED + "\nTests failed: " + e.getMessage() + RESET);
            System.exit(1);
        }
    }
} 