cmake_minimum_required(VERSION 3.20)

project(SentinelX VERSION 0.1.0 LANGUAGES CXX)

option(SENTINELX_BUILD_BENCHMARKS "Build Google Benchmark performance benchmarks" OFF)
option(SENTINELX_FETCH_GTEST "Fetch pinned GoogleTest when no package is installed" ON)
option(SENTINELX_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(SENTINELX_ENABLE_TSAN "Enable ThreadSanitizer" OFF)

if(SENTINELX_ENABLE_ASAN AND SENTINELX_ENABLE_TSAN)
    message(FATAL_ERROR "AddressSanitizer and ThreadSanitizer cannot be enabled together")
endif()

if(SENTINELX_ENABLE_ASAN OR SENTINELX_ENABLE_TSAN)
    if(MSVC)
        if(SENTINELX_ENABLE_TSAN)
            message(FATAL_ERROR "ThreadSanitizer is not supported by MSVC")
        endif()
        add_compile_options(/fsanitize=address)
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        if(SENTINELX_ENABLE_ASAN)
            add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
            add_link_options(-fsanitize=address)
        else()
            add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
            add_link_options(-fsanitize=thread)
        endif()
    else()
        message(FATAL_ERROR "Requested sanitizer is unsupported by this compiler")
    endif()
endif()

add_library(SentinelXCore
    src/file_reader/FileReader.cpp
    src/log_parser/LogParser.cpp
    src/log_analyzer/LogAnalyzer.cpp
    src/anomaly_detector/AnomalyDetector.cpp
    src/event_correlator/EventCorrelator.cpp
    src/thread_pool/ThreadPool.cpp
    src/task_queue/TaskQueue.cpp
    src/pipeline/LogPipeline.cpp
)
add_library(SentinelX::Core ALIAS SentinelXCore)

target_compile_features(SentinelXCore PUBLIC cxx_std_20)
target_include_directories(SentinelXCore
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

add_executable(sentinelx app/main.cpp)
target_link_libraries(sentinelx PRIVATE SentinelX::Core)

add_executable(sentinelx_bench benchmarks/pipeline_benchmark.cpp)
target_link_libraries(sentinelx_bench PRIVATE SentinelX::Core)
if(WIN32)
    target_link_libraries(sentinelx_bench PRIVATE Psapi)
endif()

include(CTest)
if(BUILD_TESTING)
    add_test(NAME SentinelXBenchRequiresSequentialBaseline
        COMMAND sentinelx_bench --sizes 10k --workers 2 --warmup 0 --repetitions 3)
    set_tests_properties(SentinelXBenchRequiresSequentialBaseline PROPERTIES WILL_FAIL TRUE)

    add_test(NAME SentinelXBenchRequiresThreeRepetitions
        COMMAND sentinelx_bench --sizes 10k --workers 1 --warmup 0 --repetitions 1)
    set_tests_properties(SentinelXBenchRequiresThreeRepetitions PROPERTIES WILL_FAIL TRUE)

    add_test(NAME SentinelXCliRejectsMalformedThreadCount
        COMMAND sentinelx --demo --threads 4oops)
    set_tests_properties(SentinelXCliRejectsMalformedThreadCount PROPERTIES WILL_FAIL TRUE)

    add_executable(FileReaderTest tests/file_reader_test.cpp)
    target_link_libraries(FileReaderTest PRIVATE SentinelX::Core)
    add_test(NAME FileReaderTest COMMAND FileReaderTest)

    add_executable(LogParserTest tests/log_parser_test.cpp)
    target_link_libraries(LogParserTest PRIVATE SentinelX::Core)
    add_test(NAME LogParserTest COMMAND LogParserTest)

    find_package(GTest CONFIG QUIET)
    if(NOT GTest_FOUND AND SENTINELX_FETCH_GTEST)
        include(FetchContent)
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG b514bdc898e2951020cbdca1304b75f5950d1f59
            GIT_SHALLOW TRUE
        )
        FetchContent_MakeAvailable(googletest)
    endif()

    if(TARGET GTest::gtest_main)
        add_executable(LogAnalyzerTests tests/log_analyzer_test.cpp)
        target_link_libraries(LogAnalyzerTests PRIVATE SentinelX::Core GTest::gtest_main)
        include(GoogleTest)
        gtest_discover_tests(LogAnalyzerTests DISCOVERY_TIMEOUT 30)

        add_executable(ThreadPoolTests tests/thread_pool_test.cpp)
        target_link_libraries(ThreadPoolTests PRIVATE SentinelX::Core GTest::gtest_main)
        gtest_discover_tests(ThreadPoolTests DISCOVERY_TIMEOUT 30)

        add_executable(TaskQueueTests tests/task_queue_test.cpp)
        target_link_libraries(TaskQueueTests PRIVATE SentinelX::Core GTest::gtest_main)
        gtest_discover_tests(TaskQueueTests DISCOVERY_TIMEOUT 30)

        add_executable(AnomalyDetectorTests tests/anomaly_detector_test.cpp)
        target_link_libraries(AnomalyDetectorTests PRIVATE SentinelX::Core GTest::gtest_main)
        gtest_discover_tests(AnomalyDetectorTests DISCOVERY_TIMEOUT 30)

        add_executable(EventCorrelatorTests tests/event_correlator_test.cpp)
        target_link_libraries(EventCorrelatorTests PRIVATE SentinelX::Core GTest::gtest_main)
        gtest_discover_tests(EventCorrelatorTests DISCOVERY_TIMEOUT 30)

        add_executable(LogPipelineTests tests/log_pipeline_test.cpp)
        target_link_libraries(LogPipelineTests PRIVATE SentinelX::Core GTest::gtest_main)
        gtest_discover_tests(LogPipelineTests DISCOVERY_TIMEOUT 30)
    elseif(SENTINELX_FETCH_GTEST)
        message(FATAL_ERROR "GoogleTest could not be found or fetched")
    else()
        message(WARNING "GoogleTest targets are disabled; only FileReaderTest and LogParserTest are available")
    endif()
endif()

if(SENTINELX_BUILD_BENCHMARKS)
    find_package(benchmark CONFIG QUIET)
    if(NOT benchmark_FOUND)
        include(FetchContent)
        set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
        FetchContent_Declare(
            googlebenchmark
            GIT_REPOSITORY https://github.com/google/benchmark.git
            GIT_TAG 31c66f47f60d06790a4885a261d682ba339bd100
            GIT_SHALLOW TRUE
        )
        FetchContent_MakeAvailable(googlebenchmark)
    endif()
    add_executable(SentinelXBenchmarks benchmarks/log_processing_benchmarks.cpp)
    target_link_libraries(SentinelXBenchmarks PRIVATE SentinelX::Core benchmark::benchmark_main)
    if(WIN32)
        target_link_libraries(SentinelXBenchmarks PRIVATE Psapi)
    endif()
endif()