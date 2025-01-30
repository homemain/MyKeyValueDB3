package com.andrey;

public final class Constants {
    public static final String TOMBSTONE = "TOMBSTONE";
    public static final String FILE_PREFIX = "sstable-";
    public static final String FILE_SUFFIX = ".db";
    public static final int DEFAULT_RECORD_LIMIT = 10;
    public static final String DATA_DIR = "data";
    public static final String WAL_PATH = DATA_DIR + "/wal.log";
    
    private Constants() {} // Prevent instantiation
} 