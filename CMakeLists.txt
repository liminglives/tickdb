cmake_minimum_required(VERSION 2.8)

SET(CMAKE_C_COMPILER "/usr/bin/gcc")
SET(CMAKE_CXX_COMPILER "/usr/bin/g++")

PROJECT(tickdb)
SET(TICKDB_SRC
    db/util.cpp
    db/timeline.cpp
    )

#SET(PYSRC
#    ${SRC}
#    src/py/.cpp
#    )
#INCLUDE_DIRECTORIES(/usr/include/python2.7/)
#INCLUDE_DIRECTORIES(/usr/lib/python2.7/config-x86_64-linux-gnu/)

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/include)
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/)

SET(CMAKE_CXX_FLAGS "-std=c++11 -g")

ADD_LIBRARY(tickdb STATIC ${TICKDB_SRC})
SET_TARGET_PROPERTIES(tickdb PROPERTIES OUTPUT_NAME "tickdb")


