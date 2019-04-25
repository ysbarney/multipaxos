file(GLOB comm_SRCS *.cpp *.h *.hpp)

file(GLOB comm_PUBLIC_HEADERS *.h *.hpp)

message(STATUS "comm_SRCS : " ${comm_SRCS})

set (MACOSX_RPATH ${LIBRARY_OUTPUT_PATH})

include_directories(${PROJECT_SOURCE_DIR})

add_library(comm_static STATIC ${comm_SRCS})
target_link_libraries(comm_static ${DEPENDENT_LIBRARIES})

install (
      TARGETS comm_static
      EXPORT ${PACKAGE_NAME}
      RUNTIME DESTINATION bin
      LIBRARY DESTINATION lib
      ARCHIVE DESTINATION lib)