cmake_minimum_required(VERSION 3.17)
project(${SKBUILD_PROJECT_NAME} LANGUAGES CXX)

# We require a C++17 compliant compiler
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(
  Python
  COMPONENTS Interpreter Development.Module
  REQUIRED)

if(DEFINED ENV{LAKESOUL_SOURCE_DIR})
  message(STATUS "LAKESOUL_SOURCE_DIR: $ENV{LAKESOUL_SOURCE_DIR}")
  set(LAEKSOUL_SOURCE_DIR $ENV{LAKESOUL_SOURCE_DIR})
else()
  message(FATAL_ERROR "LAKESOUL_SOURCE_DIR not defined")
endif()

execute_process(
  COMMAND ${Python_EXECUTABLE} -c "import numpy; print(numpy.get_include())"
  OUTPUT_VARIABLE NUMPY_INCLUDE_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "NUMPY include dir: ${NUMPY_INCLUDE_DIR}")

execute_process(
  COMMAND ${Python_EXECUTABLE} -c "import pyarrow; print(pyarrow.get_include())"
  OUTPUT_VARIABLE PYARROW_INCLUDE_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "PyArrow include dir: ${PYARROW_INCLUDE_DIR}")

execute_process(
  COMMAND ${Python_EXECUTABLE} -c "import pyarrow; print(pyarrow.get_library_dirs()[0])"
  OUTPUT_VARIABLE PYARROW_LIBRARY_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
message(STATUS "PyArrow library dir: ${PYARROW_LIBRARY_DIR}")

execute_process(
  COMMAND ${Python_EXECUTABLE} -c "import pyarrow; pyarrow.create_library_symlinks()"
)

add_library(arrow_shared SHARED IMPORTED)
set_target_properties(arrow_shared PROPERTIES
  IMPORTED_LOCATION "${PYARROW_LIBRARY_DIR}/libarrow${CMAKE_SHARED_LIBRARY_SUFFIX}"
  INTERFACE_INCLUDE_DIRECTORIES "${PYARROW_INCLUDE_DIR}"
)

add_library(arrow_dataset_shared SHARED IMPORTED)
set_target_properties(arrow_dataset_shared PROPERTIES
  IMPORTED_LOCATION "${PYARROW_LIBRARY_DIR}/libarrow_dataset${CMAKE_SHARED_LIBRARY_SUFFIX}"
  INTERFACE_INCLUDE_DIRECTORIES "${PYARROW_INCLUDE_DIR}"
)

add_library(arrow_python_shared SHARED IMPORTED)
set_target_properties(arrow_python_shared PROPERTIES
  IMPORTED_LOCATION "${PYARROW_LIBRARY_DIR}/libarrow_python${CMAKE_SHARED_LIBRARY_SUFFIX}"
  INTERFACE_INCLUDE_DIRECTORIES "${PYARROW_INCLUDE_DIR}"
)

add_subdirectory(lib)

# Generate .py from .proto
add_custom_command(OUTPUT ${LAEKSOUL_SOURCE_DIR}/workspace/src/lakesoul/metadata/generated/entity_pb2.py

  # COMMAND ${CMAKE_COMMAND} -E make_directory ${PROJECT_BINARY_DIR}/src/lakesoul/metadata/generated
  COMMAND Python::Interpreter -m grpc.tools.protoc -I=${LAEKSOUL_SOURCE_DIR}/rust/proto/src --python_out=${LAEKSOUL_SOURCE_DIR}/workspace/src/lakesoul/metadata/generated ${LAEKSOUL_SOURCE_DIR}/rust/proto/src/entity.proto
  MAIN_DEPENDENCY ${LAEKSOUL_SOURCE_DIR}/rust/proto/src/entity.proto
)

add_custom_command(OUTPUT _lakesoul_dataset.cpp
  COMMAND Python::Interpreter -m cython
  "${CMAKE_CURRENT_SOURCE_DIR}/src/lakesoul/arrow/_lakesoul_dataset.pyx" --cplus --output-file _lakesoul_dataset.cpp -I${PYARROW_INCLUDE_DIR}
  DEPENDS src/lakesoul/arrow/_lakesoul_dataset.pyx src/lakesoul/arrow/_lakesoul_dataset.pxd ${PROJECT_SOURCE_DIR}/src/lakesoul/metadata/generated/entity_pb2.py
  VERBATIM)

python_add_library(_lakesoul_dataset MODULE _lakesoul_dataset.cpp WITH_SOABI)

target_include_directories(_lakesoul_dataset PRIVATE
  ${PYARROW_INCLUDE_DIR}
  ${NUMPY_INCLUDE_DIR}
)
target_link_libraries(_lakesoul_dataset PRIVATE lakesoul_dataset)

install(TARGETS _lakesoul_dataset DESTINATION ${SKBUILD_PROJECT_NAME})

set_target_properties(_lakesoul_dataset PROPERTIES
  INSTALL_RPATH "@loader_path"
  BUILD_WITH_INSTALL_RPATH TRUE
)