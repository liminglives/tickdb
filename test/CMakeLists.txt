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

SET(TICKDB_TEST ${TICKDB_SRC} tickdb_test.cpp)
ADD_EXECUTABLE(tickdb_test ${TICKDB_TEST})
target_link_libraries(tickdb_test -lpthread -lgtest -lgtest_main -lrt)

SET(TICKDB_SHARED_TEST ${TICKDB_SRC} tickdb_shared.cpp)
ADD_EXECUTABLE(tickdb_shared_test ${TICKDB_SHARED_TEST})
target_link_libraries(tickdb_shared_test -lpthread -lrt)

SET(TICKDB_WATCHER_TEST ${TICKDB_SRC} tickdb_watcher.cpp)
ADD_EXECUTABLE(tickdb_watcher_test ${TICKDB_WATCHER_TEST})
target_link_libraries(tickdb_watcher_test -lpthread -lrt)

