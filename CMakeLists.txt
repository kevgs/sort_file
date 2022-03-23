cmake_minimum_required (VERSION 3.5)

project(SortFile)

set(CMAKE_CXX_STANDARD 20)

find_package(Seastar REQUIRED)

add_executable(generate_file generate_file.cc)
target_link_libraries(generate_file Seastar::seastar)

add_executable(sort_file sort_file.cc)
target_link_libraries(sort_file Seastar::seastar)
