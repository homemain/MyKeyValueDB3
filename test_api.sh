#!/bin/bash

# Color codes for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Base URL for the API
BASE_URL="http://localhost:8080"

# Function to check if the API is running
check_api() {
    echo "Checking if API is running..."
    response=$(curl -s "${BASE_URL}/ping")
    if [ "$response" = "pong" ]; then
        echo -e "${GREEN}API is running${NC}"
        return 0
    else
        echo -e "${RED}API is not running${NC}"
        return 1
    fi
}

# Test single put and get
test_single_operations() {
    echo -e "\nTesting single put/get operations..."
    
    # Test put
    curl -s -X POST "${BASE_URL}/put" -d "key=test1&value=value1"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}PUT operation successful${NC}"
    else
        echo -e "${RED}PUT operation failed${NC}"
    fi
    
    # Test get
    response=$(curl -s "${BASE_URL}/get?key=test1")
    if [ "$response" = "value1" ]; then
        echo -e "${GREEN}GET operation successful: $response${NC}"
    else
        echo -e "${RED}GET operation failed: $response${NC}"
    fi
    
    # Test delete
    curl -s -X POST "${BASE_URL}/delete" -d "key=test1"
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}DELETE operation successful${NC}"
    else
        echo -e "${RED}DELETE operation failed${NC}"
    fi
    
    # Verify delete
    response=$(curl -s -w "%{http_code}" "${BASE_URL}/get?key=test1")
    if [[ "$response" == *"404"* ]]; then
        echo -e "${GREEN}DELETE verification successful${NC}"
    else
        echo -e "${RED}DELETE verification failed${NC}"
    fi
}

# Test batch operations
test_batch_operations() {
    echo -e "\nTesting batch operations..."
    
    # Test putBatch
    curl -s -X POST "${BASE_URL}/putbatch" \
        -H "Content-Type: application/json" \
        -d '{
            "batch1": "value1",
            "batch2": "value2",
            "batch3": "value3"
        }'
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}PUT batch operation successful${NC}"
    else
        echo -e "${RED}PUT batch operation failed${NC}"
    fi
    
    # Test getBatch
    response=$(curl -s "${BASE_URL}/getbatch?keyStart=batch1&keyEnd=batch3")
    if [[ "$response" == *"batch1"* && "$response" == *"value1"* ]]; then
        echo -e "${GREEN}GET batch operation successful: $response${NC}"
    else
        echo -e "${RED}GET batch operation failed: $response${NC}"
    fi
}

# Main test execution
main() {
    echo "Starting API tests..."
    
    # Check if API is running
    check_api
    if [ $? -ne 0 ]; then
        echo "Please start the API server first"
        exit 1
    fi
    
    # Run tests
    test_single_operations
    test_batch_operations
    
    echo -e "\nAll tests completed"
}

# Run main function
main 