cmake_minimum_required(VERSION 3.15)
project(${SKBUILD_PROJECT_NAME} LANGUAGES CXX)

# We require a C++17 compliant compiler
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 CONFIG REQUIRED)

add_subdirectory(lib)

pybind11_add_module(_core MODULE src/main.cpp)

target_link_libraries(_core PRIVATE lakesoul_dataset)

install(TARGETS _core DESTINATION ${SKBUILD_PROJECT_NAME})

set_target_properties(_core PROPERTIES
    INSTALL_RPATH "@loader_path"
    BUILD_WITH_INSTALL_RPATH TRUE
)