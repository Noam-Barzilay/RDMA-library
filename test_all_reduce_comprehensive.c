#include "rdma_collective_comm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

// Test configuration
#define MAX_RANKS 4
#define MAX_ELEMENTS 1024

// Global test configuration
static int test_rank;
static int test_size;
static int test_count;
static int test_chunk_size;

// Helper function to print array
void print_array(const char* prefix, const int* arr, int count) {
    printf("[%s] ", prefix);
    for (int i = 0; i < count; i++) {
        printf("%d ", arr[i]);
    }
    printf("\n");
}

// Helper function to verify all-reduce result
int verify_all_reduce_result(const int* result, const int* expected, int count, const char* test_name) {
    printf("[VERIFY] %s: Checking %d elements\n", test_name, count);
    
    int errors = 0;
    for (int i = 0; i < count; i++) {
        if (result[i] != expected[i]) {
            printf("[ERROR] %s: Element %d: expected %d, got %d\n", test_name, i, expected[i], result[i]);
            errors++;
            if (errors > 10) { // Limit error output
                printf("[ERROR] %s: Too many errors, stopping output\n", test_name);
                break;
            }
        }
    }
    
    if (errors == 0) {
        printf("[PASS] %s: All %d elements correct\n", test_name, count);
        return 1;
    } else {
        printf("[FAIL] %s: %d errors found\n", test_name, errors);
        return 0;
    }
}

// Helper function to calculate expected result
void calculate_expected_result(int* expected, int count, int size) {
    // Each rank contributes: rank * 1000 + element_index
    for (int i = 0; i < count; i++) {
        expected[i] = 0;
        for (int r = 0; r < size; r++) {
            expected[i] += (r * 1000 + i);
        }
    }
}

// Helper function to calculate expected result for multiplication
void calculate_expected_mult_result(int* expected, int count, int size) {
    // Each rank contributes: rank + 1 (to avoid zeros in multiplication)
    for (int i = 0; i < count; i++) {
        expected[i] = 1;
        for (int r = 0; r < size; r++) {
            expected[i] *= (r + 1); // rank 0 contributes 1, rank 1 contributes 2, etc.
        }
    }
}

// Test 1: Basic 2-rank test with small data (should use EAGER)
void test_basic_2rank_eager(void* pg_handle) {
    printf("\n=== TEST 1: Basic 2-Rank Eager Protocol ===\n");
    
    const int count = 4; // Small enough for eager: 4 * 4 = 16 bytes < 4096 bytes
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    // Initialize with rank-specific data
    for (int i = 0; i < count; i++) {
        sendbuf[i] = test_rank * 1000 + i;
    }
    
    print_array("Initial", sendbuf, count);
    
    // Perform all-reduce
    if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_SUM, pg_handle) != 0) {
        fprintf(stderr, "All-reduce failed\n");
        free(sendbuf);
        free(recvbuf);
        free(expected);
        return;
    }
    
    print_array("Result", recvbuf, count);
    
    // Calculate expected result
    calculate_expected_result(expected, count, test_size);
    print_array("Expected", expected, count);
    
    // Verify result
    verify_all_reduce_result(recvbuf, expected, count, "Basic 2-Rank Eager");
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

// Test 2: Basic 2-rank test with large data (should use RENDEZVOUS)
void test_basic_2rank_rendezvous(void* pg_handle) {
    printf("\n=== TEST 2: Basic 2-Rank Rendezvous Protocol ===\n");
    
    const int count = 2048; // Large enough for rendezvous: 2048 * 4 = 8192 bytes > 4096 bytes
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    // Initialize with rank-specific data
    for (int i = 0; i < count; i++) {
        sendbuf[i] = test_rank * 1000 + i;
    }
    
    print_array("Initial", sendbuf, count);
    
    // Perform all-reduce
    if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_SUM, pg_handle) != 0) {
        fprintf(stderr, "All-reduce failed\n");
        free(sendbuf);
        free(recvbuf);
        free(expected);
        return;
    }
    
    print_array("Result", recvbuf, count);
    
    // Calculate expected result
    calculate_expected_result(expected, count, test_size);
    print_array("Expected", expected, count);
    
    // Verify result
    verify_all_reduce_result(recvbuf, expected, count, "Basic 2-Rank Rendezvous");
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

// Test 3: Multiple all-reduce operations (stress test)
void test_multiple_operations(void* pg_handle) {
    printf("\n=== TEST 3: Multiple All-Reduce Operations ===\n");
    
    const int count = 8;
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    int all_passed = 1;
    
    // Run multiple all-reduce operations
    for (int op = 0; op < 3; op++) {
        printf("\n--- Operation %d ---\n", op + 1);
        
        // Initialize with different data for each operation
        for (int i = 0; i < count; i++) {
            sendbuf[i] = test_rank * 1000 + i + op * 100;
        }
        
        print_array("Initial", sendbuf, count);
        
        // Perform all-reduce
        if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_SUM, pg_handle) != 0) {
            fprintf(stderr, "All-reduce failed in operation %d\n", op + 1);
            all_passed = 0;
            break;
        }
        
        print_array("Result", recvbuf, count);
        
        // Calculate expected result
        calculate_expected_result(expected, count, test_size);
        for (int i = 0; i < count; i++) {
            expected[i] += op * 100 * test_size;
        }
        print_array("Expected", expected, count);
        
        // Verify result
        if (!verify_all_reduce_result(recvbuf, expected, count, "Multiple Operations")) {
            all_passed = 0;
        }
    }
    
    if (all_passed) {
        printf("[PASS] All multiple operations completed successfully\n");
    } else {
        printf("[FAIL] Some operations failed\n");
    }
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

// Test 4: Large data test (stress test for rendezvous)
void test_large_data(void* pg_handle) {
    printf("\n=== TEST 4: Large Data Test ===\n");
    
    const int count = 1536; // Large data: 1536 * 4 = 6144 bytes > 4096 bytes (rendezvous)
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    // Initialize with rank-specific data
    for (int i = 0; i < count; i++) {
        sendbuf[i] = test_rank * 1000 + i;
    }
    
    printf("Testing with %d elements (large data for rendezvous)\n", count);
    
    // Perform all-reduce
    if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_SUM, pg_handle) != 0) {
        fprintf(stderr, "All-reduce failed\n");
        free(sendbuf);
        free(recvbuf);
        free(expected);
        return;
    }
    
    // Calculate expected result
    calculate_expected_result(expected, count, test_size);
    
    // Verify result
    verify_all_reduce_result(recvbuf, expected, count, "Large Data Test");
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

// Test 5: Edge case: single element per rank
void test_single_element(void* pg_handle) {
    printf("\n=== TEST 5: Single Element Per Rank ===\n");
    
    const int count = 2; // One element per rank for 2 ranks
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    // Initialize with rank-specific data
    for (int i = 0; i < count; i++) {
        sendbuf[i] = test_rank * 1000 + i;
    }
    
    print_array("Initial", sendbuf, count);
    
    // Perform all-reduce
    if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_SUM, pg_handle) != 0) {
        fprintf(stderr, "All-reduce failed\n");
        free(sendbuf);
        free(recvbuf);
        free(expected);
        return;
    }
    
    print_array("Result", recvbuf, count);
    
    // Calculate expected result
    calculate_expected_result(expected, count, test_size);
    print_array("Expected", expected, count);
    
    // Verify result
    verify_all_reduce_result(recvbuf, expected, count, "Single Element Test");
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

// Test 6: Edge case: odd number of elements
void test_odd_elements(void* pg_handle) {
    printf("\n=== TEST 6: Odd Number of Elements ===\n");
    
    const int count = 6; // Odd division: 3 elements per rank
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    // Initialize with rank-specific data
    for (int i = 0; i < count; i++) {
        sendbuf[i] = test_rank * 1000 + i;
    }
    
    print_array("Initial", sendbuf, count);
    
    // Perform all-reduce
    if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_SUM, pg_handle) != 0) {
        fprintf(stderr, "All-reduce failed\n");
        free(sendbuf);
        free(recvbuf);
        free(expected);
        return;
    }
    
    print_array("Result", recvbuf, count);
    
    // Calculate expected result
    calculate_expected_result(expected, count, test_size);
    print_array("Expected", expected, count);
    
    // Verify result
    verify_all_reduce_result(recvbuf, expected, count, "Odd Elements Test");
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

// Test 7: Protocol boundary test (test around EAGER_THRESHOLD)
void test_protocol_boundary(void* pg_handle) {
    printf("\n=== TEST 7: Protocol Boundary Test ===\n");
    
    // Test sizes around the EAGER_THRESHOLD (4096 bytes = 1024 ints)
    // Test just below, at, and above the threshold
    int test_sizes[] = {1020, 1024, 1028, 1200, 1500, 2000};  // 1024 ints = 4096 bytes exactly
    int num_tests = sizeof(test_sizes) / sizeof(test_sizes[0]);
    
    int all_passed = 1;
    
    for (int t = 0; t < num_tests; t++) {
        int count = test_sizes[t];
        int message_size = count * sizeof(int);
        const char* expected_protocol = (message_size <= 4096) ? "EAGER" : "RENDEZVOUS";
        printf("\n--- Testing with %d elements (%d bytes) - Expected: %s ---\n", 
               count, message_size, expected_protocol);
        
        int* sendbuf = malloc(count * sizeof(int));
        int* recvbuf = malloc(count * sizeof(int));
        int* expected = malloc(count * sizeof(int));
        
        // Initialize with rank-specific data
        for (int i = 0; i < count; i++) {
            sendbuf[i] = test_rank * 1000 + i;
        }
        
        // Perform all-reduce
        if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_SUM, pg_handle) != 0) {
            fprintf(stderr, "All-reduce failed for size %d\n", count);
            all_passed = 0;
            free(sendbuf);
            free(recvbuf);
            free(expected);
            continue;
        }
        
        // Calculate expected result
        calculate_expected_result(expected, count, test_size);
        
        // Verify result
        char test_name[100];
        snprintf(test_name, sizeof(test_name), "Protocol Boundary Test (%d elements)", count);
        if (!verify_all_reduce_result(recvbuf, expected, count, test_name)) {
            all_passed = 0;
        }
        
        free(sendbuf);
        free(recvbuf);
        free(expected);
    }
    
    if (all_passed) {
        printf("[PASS] All protocol boundary tests passed\n");
    } else {
        printf("[FAIL] Some protocol boundary tests failed\n");
    }
}

// Test 8: Stress test with maximum data
void test_stress_maximum(void* pg_handle) {
    printf("\n=== TEST 8: Stress Test with Maximum Data ===\n");
    
    const int count = 4096; // Large stress test: 4096 * 4 = 16384 bytes > 4096 bytes (rendezvous)
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    // Initialize with rank-specific data
    for (int i = 0; i < count; i++) {
        sendbuf[i] = test_rank * 1000 + i;
    }
    
    printf("Stress testing with %d elements\n", count);
    
    // Perform all-reduce
    if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_SUM, pg_handle) != 0) {
        fprintf(stderr, "All-reduce failed in stress test\n");
        free(sendbuf);
        free(recvbuf);
        free(expected);
        return;
    }
    
    // Calculate expected result
    calculate_expected_result(expected, count, test_size);
    
    // Verify result
    verify_all_reduce_result(recvbuf, expected, count, "Stress Test");
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

// Test 9: Basic multiplication test with small data (EAGER)
void test_basic_mult_eager(void* pg_handle) {
    printf("\n=== TEST 9: Basic Multiplication Eager Protocol ===\n");
    
    const int count = 4; // Small enough for eager
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    // Initialize with rank+1 to avoid zeros in multiplication
    for (int i = 0; i < count; i++) {
        sendbuf[i] = test_rank + 1; // rank 0 -> 1, rank 1 -> 2, etc.
    }
    
    print_array("Initial", sendbuf, count);
    
    // Perform all-reduce with multiplication
    if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_MULT, pg_handle) != 0) {
        fprintf(stderr, "All-reduce multiplication failed\n");
        free(sendbuf);
        free(recvbuf);
        free(expected);
        return;
    }
    
    print_array("Result", recvbuf, count);
    
    // Calculate expected result for multiplication
    calculate_expected_mult_result(expected, count, test_size);
    print_array("Expected", expected, count);
    
    // Verify result
    verify_all_reduce_result(recvbuf, expected, count, "Basic Multiplication Eager");
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

// Test 10: Basic multiplication test with large data (RENDEZVOUS)
void test_basic_mult_rendezvous(void* pg_handle) {
    printf("\n=== TEST 10: Basic Multiplication Rendezvous Protocol ===\n");
    
    const int count = 2048; // Large enough for rendezvous: 2048 * 4 = 8192 bytes > 4096 bytes
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    // Initialize with rank+1 to avoid zeros in multiplication
    for (int i = 0; i < count; i++) {
        sendbuf[i] = test_rank + 1; // rank 0 -> 1, rank 1 -> 2, etc.
    }
    
    print_array("Initial", sendbuf, count);
    
    // Perform all-reduce with multiplication
    if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_MULT, pg_handle) != 0) {
        fprintf(stderr, "All-reduce multiplication failed\n");
        free(sendbuf);
        free(recvbuf);
        free(expected);
        return;
    }
    
    print_array("Result", recvbuf, count);
    
    // Calculate expected result for multiplication
    calculate_expected_mult_result(expected, count, test_size);
    print_array("Expected", expected, count);
    
    // Verify result
    verify_all_reduce_result(recvbuf, expected, count, "Basic Multiplication Rendezvous");
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

// Test 11: Mixed operations test (alternating SUM and MULT)
void test_mixed_operations(void* pg_handle) {
    printf("\n=== TEST 11: Mixed Operations Test ===\n");
    
    const int count = 8;
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    int all_passed = 1;
    
    // Test alternating between SUM and MULT operations
    OPERATION operations[] = {OP_SUM, OP_MULT, OP_SUM, OP_MULT};
    const char* op_names[] = {"SUM", "MULT", "SUM", "MULT"};
    int num_ops = sizeof(operations) / sizeof(operations[0]);
    
    for (int op = 0; op < num_ops; op++) {
        printf("\n--- Operation %d: %s ---\n", op + 1, op_names[op]);
        
        // Initialize data based on operation type
        for (int i = 0; i < count; i++) {
            if (operations[op] == OP_SUM) {
                sendbuf[i] = test_rank * 1000 + i + op * 100;
            } else { // OP_MULT
                sendbuf[i] = test_rank + 1; // Avoid zeros
            }
        }
        
        print_array("Initial", sendbuf, count);
        
        // Perform all-reduce with current operation
        if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, operations[op], pg_handle) != 0) {
            fprintf(stderr, "All-reduce failed in operation %d (%s)\n", op + 1, op_names[op]);
            all_passed = 0;
            break;
        }
        
        print_array("Result", recvbuf, count);
        
        // Calculate expected result based on operation type
        if (operations[op] == OP_SUM) {
            calculate_expected_result(expected, count, test_size);
            for (int i = 0; i < count; i++) {
                expected[i] += op * 100 * test_size;
            }
        } else { // OP_MULT
            calculate_expected_mult_result(expected, count, test_size);
        }
        print_array("Expected", expected, count);
        
        // Verify result
        char test_name[100];
        snprintf(test_name, sizeof(test_name), "Mixed Operations %s", op_names[op]);
        if (!verify_all_reduce_result(recvbuf, expected, count, test_name)) {
            all_passed = 0;
        }
    }
    
    if (all_passed) {
        printf("[PASS] All mixed operations completed successfully\n");
    } else {
        printf("[FAIL] Some mixed operations failed\n");
    }
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

// Test 12: Edge case multiplication with different values
void test_mult_edge_cases(void* pg_handle) {
    printf("\n=== TEST 12: Multiplication Edge Cases ===\n");
    
    const int count = 6;
    int* sendbuf = malloc(count * sizeof(int));
    int* recvbuf = malloc(count * sizeof(int));
    int* expected = malloc(count * sizeof(int));
    
    // Initialize with small prime numbers to test multiplication precision
    int base_values[] = {2, 3, 5, 7, 11, 13};
    for (int i = 0; i < count; i++) {
        sendbuf[i] = base_values[i] + test_rank; // Each rank adds its rank to the base
    }
    
    print_array("Initial", sendbuf, count);
    
    // Perform all-reduce with multiplication
    if (pg_all_reduce(sendbuf, recvbuf, count, DATATYPE_INT, OP_MULT, pg_handle) != 0) {
        fprintf(stderr, "All-reduce multiplication failed\n");
        free(sendbuf);
        free(recvbuf);
        free(expected);
        return;
    }
    
    print_array("Result", recvbuf, count);
    
    // Calculate expected result manually for this specific case
    for (int i = 0; i < count; i++) {
        expected[i] = 1;
        for (int r = 0; r < test_size; r++) {
            expected[i] *= (base_values[i] + r);
        }
    }
    print_array("Expected", expected, count);
    
    // Verify result
    verify_all_reduce_result(recvbuf, expected, count, "Multiplication Edge Cases");
    
    free(sendbuf);
    free(recvbuf);
    free(expected);
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <server_list>\n", argv[0]);
        fprintf(stderr, "Example: %s \"mlxstud01,mlxstud02\"\n", argv[0]);
        return 1;
    }

    // Connect to process group
    void *pg_handle;
    if (connect_process_group(argv[1], &pg_handle) != 0) {
        fprintf(stderr, "Failed to connect to process group\n");
        return 1;
    }

    pg_handle_t *handle = (pg_handle_t *)pg_handle;
    test_rank = handle->rank;
    test_size = handle->size;
    test_count = 4; // Default test count
    test_chunk_size = test_count / test_size;

    printf("[COMPREHENSIVE TEST] Rank %d/%d: Starting comprehensive all-reduce tests\n", test_rank, test_size);
    printf("[COMPREHENSIVE TEST] EAGER_THRESHOLD = %d bytes\n", EAGER_THRESHOLD);
    printf("[COMPREHENSIVE TEST] Test suite includes: SUM and MULT operations, eager and rendezvous protocols\n");

    // Run all tests
    test_basic_2rank_eager(pg_handle);
    test_basic_2rank_rendezvous(pg_handle);
    test_multiple_operations(pg_handle);
    test_large_data(pg_handle);
    test_single_element(pg_handle);
    test_odd_elements(pg_handle);
    test_protocol_boundary(pg_handle);
    test_stress_maximum(pg_handle);
    
    // Multiplication operation tests
    test_basic_mult_eager(pg_handle);
    test_basic_mult_rendezvous(pg_handle);
    test_mixed_operations(pg_handle);
    test_mult_edge_cases(pg_handle);

    printf("\n[COMPREHENSIVE TEST] Rank %d: All tests completed\n", test_rank);
    printf("[COMPREHENSIVE TEST] Total tests run: 12 (8 SUM + 4 MULT operations)\n");
    
    // Clean up
    pg_close(pg_handle);
    return 0;
} 