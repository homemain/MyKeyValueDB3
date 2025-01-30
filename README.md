# Key-Value Storage Engine

A simple and a bit naive implementation of a key-value storage engine implemented in Java, using only standard Java libraries. It supports  persistent storage, write-ahead logging, and concurrent access.
Project aimed to be a learning exercise for me in several Java areas as well as AI usage in code generation.

## Overview

This project implements a key-value storage engine with the following features:
- REST API interface
- Persistent storage using SSTable files
- Write-ahead logging (WAL) for durability
- Memory table (MemTable) for fast access
- Support for batch operations
- Thread-safe operations

## Architecture

The storage engine follows a layered architecture:

1. **API Layer** (`MyAPILayer.java`)
   - Handles HTTP requests
   - Provides REST endpoints for operations
   - Converts HTTP requests to storage operations

2. **Storage Engine** (`MyStorageEngine.java`)
   - Core component coordinating all operations
   - Manages MemTable and SSTable interactions
   - Ensures data consistency and durability

3. **Memory Table** (`MyMemTable.java`)
   - In-memory storage using ConcurrentSkipListMap
   - Provides fast read/write access
   - Automatically flushes to disk when full

4. **SSTable** (`MySSTable.java`)
   - On-disk storage format
   - Manages multiple SSTable files
   - Handles file operations and searching

5. **Write-Ahead Log** (`MyWriteAheadLog.java`)
   - Ensures durability of operations
   - Records all modifications before they are applied
   - Supports recovery after crashes

## API Endpoints

- `GET /ping` - Health check
- `POST /put` - Store a key-value pair
- `GET /get` - Retrieve a value by key
- `POST /delete` - Delete a key-value pair
- `POST /putbatch` - Store multiple key-value pairs
- `GET /getbatch` - Retrieve values for a range of keys
- `POST /shutdown` - Gracefully shut down the server

## How to Run

1. **Compile the Project**
   ```bash
   javac com/andrey/*.java
   ```

2. **Start the Server**
   ```bash
   java com.andrey.MyKeyValueDB
   ```
   The server will start on port 8080 by default.

3. **Run Tests**
   ```bash
   # Run Java tests
   java com.andrey.MyTestCases # Runs test cases including:
   # - Basic put/get/delete operations
   # - Batch operations with key ranges
   # - Persistence across restarts
   # - Concurrent access with multiple threads
   # - Memory table flushing
   
   
   # Run basic external API tests (once server is running)
   ./test_api.sh
   ```

## Usage Examples

1. **Store a Value**
   ```bash
   curl -X POST "http://localhost:8080/put" -d "key=mykey&value=myvalue"
   ```

2. **Retrieve a Value**
   ```bash
   curl "http://localhost:8080/get?key=mykey"
   ```

3. **Store Multiple Values**
   ```bash
   curl -X POST "http://localhost:8080/putbatch" \
        -H "Content-Type: application/json" \
        -d '{"key1":"value1","key2":"value2"}'
   ```

4. **Retrieve a Range of Values**
   ```bash
   curl "http://localhost:8080/getbatch?keyStart=key1&keyEnd=key2"
   ```

## Configuration

Default configuration values are stored in `Constants.java`:
- MemTable size limit
- Data directory path
- WAL file path
- File naming patterns