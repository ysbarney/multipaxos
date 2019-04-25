cmake_minimum_required (VERSION 2.8)

project (safe-multipaxos C CXX)

if (UNIX)
    if(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE "Release")
    endif()

    set (CXX_FLAGS
         -g
         -fPIC
         -Wall
         -Wno-unused-parameter
         -Wno-unused-function
         -Wunused-variable
         -Wunused-value
         -Wshadow
         -Wcast-qual
         -Wcast-align
         -Wwrite-strings
         -Wsign-compare
         -Winvalid-pch
	 -Wignored-qualifiers
         -fms-extensions
         -Wfloat-equal
         -Wextra
         -std=c++11
    )
    include_directories (${PROJECT_SOURCE_DIR}
                         ${PROJECT_SOURCE_DIR}/3rdparty
			 ${PROJECT_SOURCE_DIR}/3rdparty/rocksdb/include
			 ${PROJECT_SOURCE_DIR}/3rdparty/concurrentqueue
			 ${PROJECT_SOURCE_DIR}/3rdparty/protobuf/include
			 ${PROJECT_SOURCE_DIR}/include
			 ${PROJECT_SOURCE_DIR}/comm
			/data/home/lys/workspace/tbb-2018_U6/include
			/data/home/lys/workspace/publiclib/skiplist
			/data/home/lys/workspace/publiclib/memory
			/data/home/lys/workspace/publiclib/port
			/data/home/lys/workspace/publiclib/util)
else (UNIX)
set (CXX_FLAGS -DWIN32 -D_WIN32 -EHsc)
    set(VcpkgRoot ${_VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET})
    include_directories(${VcpkgRoot}/include
                        ${PROJECT_SOURCE_DIR}
                        ${PROJECT_SOURCE_DIR}/3rdparty/wininclude)

    # On windows, Debug mode
    link_directories(${VcpkgRoot}/debug/lib ${VcpkgRoot}/debug/lib/manual-link)

    # On Windows, Release mode
    # link_directories(${VcpkgRoot}/lib ${VcpkgRoot}/lib/manual-link)
endif (UNIX)

string (REPLACE ";" " " CMAKE_CXX_FLAGS "${CXX_FLAGS}")

if (UNIX)
    SET(CMAKE_CXX_FLAGS_DEBUG   "-O0 -g -ggdb -D_DEBUG")
    SET(CMAKE_CXX_FLAGS_RELEASE "-O3 -g -ggdb -DNDEBUG")
    link_directories("/data/home/lys/workspace/publiclib"
			${PROJECT_SOURCE_DIR}/3rdparty/protobuf/lib
			${PROJECT_SOURCE_DIR}/3rdparty/rocksdb)
    
    SET(DEPENDENT_LIBRARIES pthread dl z libevent.a libprotobuf.a librocksdb.a libboost_log_setup.a libskiplist.a libboost_log.a libboost_filesystem.a libboost_thread.a libboost_date_time.a libboost_system.a libboost_regex.a)
else (UNIX)
    SET(DEPENDENT_LIBRARIES event)
endif (UNIX)

if (CMAKE_BENCHMARK_TESTING)
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DH_BENCHMARK_TESTING=1")
endif (CMAKE_BENCHMARK_TESTING)

set (EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set (LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

#set (MACOSX_RPATH ${LIBRARY_OUTPUT_PATH})
set (CMAKE_MACOSX_RPATH 0)

find_program(THRIFT_COMPILER thrift)
find_path(THRIFT_INCLUDE_DIR thrift)
find_library(THRIFT_LIBRARY NAMES thrift)

message (STATUS "This is BINARY dir " ${PROJECT_BINARY_DIR})
message (STATUS "This is SOURCE dir " ${PROJECT_SOURCE_DIR})
message (STATUS "CMAKE_CXX_FLAGS " ${CMAKE_CXX_FLAGS})
message (STATUS "EXECUTABLE_OUTPUT_PATH " ${EXECUTABLE_OUTPUT_PATH})
message (STATUS "LIBRARY_OUTPUT_PATH " ${LIBRARY_OUTPUT_PATH})
message (STATUS "THRIFT_COMPILER " ${THRIFT_COMPILER})
message (STATUS "THRIFT_INCLUDE_DIR " ${THRIFT_INCLUDE_DIR})
message (STATUS "THRIFT_LIBRARY " ${THRIFT_LIBRARY})

add_subdirectory (comm)
add_subdirectory (network)
add_subdirectory (paxos)
add_subdirectory (zkserver)
#add_subdirectory (examples)

set (CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

