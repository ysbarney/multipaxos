file(GLOB zkserver_SRCS *.cpp *.h *.c *.cc)

file(GLOB zkserver_PUBLIC_HEADERS *.h)

message(STATUS "zkserver_SRCS : " ${zkserver_SRCS})

set (MACOSX_RPATH ${LIBRARY_OUTPUT_PATH})

include_directories(${PROJECT_SOURCE_DIR}/network 
			${PROJECT_SOURCE_DIR}/comm 
			${PROJECT_SOURCE_DIR}/zkserver 
			${PROJECT_SOURCE_DIR}/3rdparty)

set(LIBRARIES paxos_node_static paxos_algorithm_static paxos_checkpoint_static paxos_config_static paxos_master_static paxos_network_static paxos_smbase_static paxos_storage_static  paxos_comm_static network_boost_static comm_static ${DEPENDENT_LIBRARIES})
if (UNIX)
else(UNIX)
link_directories(${PROJECT_SOURCE_DIR}/vsprojects/bin/${CMAKE_BUILD_TYPE}/
                                 ${PROJECT_SOURCE_DIR}/3rdparty/glog-0.3.4/${CMAKE_BUILD_TYPE})
endif(UNIX)

add_executable(zkserver ${zkserver_SRCS})
target_link_libraries(zkserver ${LIBRARIES})

if (UNIX)
    install (
      TARGETS zkserver

      EXPORT ${PACKAGE_NAME}
      RUNTIME DESTINATION bin
      LIBRARY DESTINATION lib
      ARCHIVE DESTINATION lib)

else (UNIX)
    install (
      TARGETS zkserver
      EXPORT ${PACKAGE_NAME}
      RUNTIME DESTINATION bin
      LIBRARY DESTINATION lib
      ARCHIVE DESTINATION lib)

endif (UNIX)
