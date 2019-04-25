file(GLOB network_SRCS *.cpp *.h )

file(GLOB network_PUBLIC_HEADERS *.h)

message(STATUS "network_SRCS : " ${network_SRCS})

set (MACOSX_RPATH ${LIBRARY_OUTPUT_PATH})

include_directories(${PROJECT_SOURCE_DIR} 
					${PROJECT_SOURCE_DIR}/comm)

add_library(network_static STATIC ${network_SRCS})
target_link_libraries(network_static ${DEPENDENT_LIBRARIES})

if (UNIX)
    # boost lockfree queue
    add_library(network_boost_static STATIC ${network_SRCS})
    target_compile_definitions(network_boost_static PRIVATE -DH_HAVE_BOOST=1)
    target_link_libraries(network_boost_static ${DEPENDENT_LIBRARIES})

    # https://github.com/cameron314/concurrentqueue
    add_library(network_concurrentqueue_static STATIC ${network_SRCS})
    target_compile_definitions(network_concurrentqueue_static PRIVATE -DH_HAVE_CAMERON314_CONCURRENTQUEUE=1)
    target_link_libraries(network_concurrentqueue_static ${DEPENDENT_LIBRARIES})

    set (CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
    #include (utils)
    #include (packages)

    install (
      TARGETS network_static network_boost_static network_concurrentqueue_static

      EXPORT ${PACKAGE_NAME}
      RUNTIME DESTINATION bin
      LIBRARY DESTINATION lib
      ARCHIVE DESTINATION lib)

else (UNIX)
    install (
      TARGETS network_static
      EXPORT ${PACKAGE_NAME}
      RUNTIME DESTINATION bin
      LIBRARY DESTINATION lib
      ARCHIVE DESTINATION lib)

endif (UNIX)