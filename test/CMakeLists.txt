cmake_minimum_required(VERSION 2.8)

SET(CMAKE_C_COMPILER "/usr/bin/gcc")
SET(CMAKE_CXX_COMPILER "/usr/bin/g++")

PROJECT(test_tickdb)

IF (APPLE)
    MESSAGE(STATUS "mac")
ELSEIF (UNIX)
    MESSAGE(STATUS "linux")
ENDIF()


SET(TICKDB_SRC
    ${CMAKE_CURRENT_SOURCE_DIR}/../db/util.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/../db/timeline.cpp
    )

#file(GLOB CPP_SRC_LIST "${CMAKE_CURRENT_SOURCE_DIR}/../src/*.cpp")
#MESSAGE("${CPP_SRC_LIST}")


INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../)

IF (APPLE)
    INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/gtest_mac/include)
    LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/gtest_mac/lib/)
ELSEIF (UNIX)
    INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/gtest/include)
    LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/gtest/lib/)
ENDIF()

#INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/zlib/include)

#LINK_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/zlib/lib)

#SET(CMAKE_CXX_FLAGS "-std=c++11 -g -pthread -ftest-coverage -fprofile-arcs -Wno-deprecated")
SET(CMAKE_CXX_FLAGS "-std=c++11 -g -pthread")

SET(TEST_TICKDB ${TICKDB_SRC} test_tickdb.cpp)
ADD_EXECUTABLE(test_tickdb ${TEST_TICKDB})
target_link_libraries(test_tickdb -lpthread -lgtest -lgtest_main)

