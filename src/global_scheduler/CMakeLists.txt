cmake_minimum_required(VERSION 2.8)

project(global_scheduler)

include(${CMAKE_CURRENT_LIST_DIR}/../common/cmake/Common.cmake)

SET_SOURCE_FILES_PROPERTIES(global_scheduler.c global_scheduler_algorithm.c PROPERTIES LANGUAGE CXX)

add_executable(global_scheduler global_scheduler.c global_scheduler_algorithm.c)
target_link_libraries(global_scheduler common ${HIREDIS_LIB})
