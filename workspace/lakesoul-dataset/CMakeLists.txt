cmake_minimum_required(VERSION 3.17)
project(${SKBUILD_PROJECT_NAME} LANGUAGES CXX)

# We require a C++17 compliant compiler
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# set(PYBIND11_FINDPYTHON ON)
# find_package(pybind11 CONFIG REQUIRED)
# pybind11_add_module(_core MODULE src/main.cpp)
# target_link_libraries(_core PRIVATE lakesoul_dataset)

find_package(
  Python
  COMPONENTS Interpreter Development.Module
  REQUIRED)

add_custom_command(
  OUTPUT example.cpp
  COMMAND Python::Interpreter -m cython
          "${CMAKE_CURRENT_SOURCE_DIR}/src/example.pyx" --cplus --output-file example.cpp
  DEPENDS src/example.pyx
  VERBATIM)

python_add_library(example MODULE example.cpp WITH_SOABI)

add_subdirectory(lib)

target_link_libraries(example PRIVATE lakesoul_dataset)

install(TARGETS example DESTINATION ${SKBUILD_PROJECT_NAME})

set_target_properties(example PROPERTIES
    INSTALL_RPATH "@loader_path"
    BUILD_WITH_INSTALL_RPATH TRUE
)