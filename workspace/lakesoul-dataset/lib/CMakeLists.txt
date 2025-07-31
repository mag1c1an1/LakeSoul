# SPDX-FileCopyrightText: LakeSoul Contributors
# 
# SPDX-License-Identifier: Apache-2.0

# Find Cargo 
find_program(Cargo_EXECUTABLE cargo)
if(NOT Cargo_EXECUTABLE)
    message(FATAL_ERROR "Cargo not found")
endif()
message(STATUS "Found Cargo at ${Cargo_EXECUTABLE}")
execute_process(COMMAND ${Cargo_EXECUTABLE} --version
                RESULT_VARIABLE rc
                OUTPUT_VARIABLE cargo_version)
if(NOT rc EQUAL 0)
    message(FATAL_ERROR "Fail to get Cargo version")
endif()

# Build liblakesoul_io_c.so

message(${PROJECT_SOURCE_DIR})

set(lakesoul_io_fullname liblakesoul_io_c${CMAKE_SHARED_LIBRARY_SUFFIX})


if(NOT EXISTS ${PROJECT_SOURCE_DIR}/../../rust/target/release/${lakesoul_io_fullname})
    execute_process(COMMAND ${Cargo_EXECUTABLE} build -r
                    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/../../rust/lakesoul-io-c
                    RESULT_VARIABLE rc)
    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "Fail to build liblakesoul_io_c.so")
    endif()
endif()

# # Build liblakesoul_metadata_c.so
# if(NOT EXISTS ${PROJECT_SOURCE_DIR}/../rust/target/release/liblakesoul_metadata_c.so)
#     execute_process(COMMAND ${Cargo_EXECUTABLE} build -r
#                     WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/../rust/lakesoul-metadata-c
#                     RESULT_VARIABLE rc)
#     if(NOT rc EQUAL 0)
#         message(FATAL_ERROR "Fail to build liblakesoul_metadata_c.so")
#     endif()
# endif()

# message(STATUS "Arrow version: ${ARROW_VERSION}")
# message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")

# Create CMake imported targets
add_library(liblakesoul_io_c SHARED IMPORTED)
set_target_properties(liblakesoul_io_c PROPERTIES
    IMPORTED_LOCATION "${PROJECT_SOURCE_DIR}/../../rust/target/release/${lakesoul_io_fullname}"
    INTERFACE_INCLUDE_DIRECTORIES "${PROJECT_SOURCE_DIR}/../../rust/lakesoul-io-c"
    IMPORTED_NO_SONAME TRUE)

find_package(Arrow REQUIRED)
add_library(lakesoul_dataset SHARED data_reader.cpp example.cpp)
target_include_directories(lakesoul_dataset PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(lakesoul_dataset PUBLIC Arrow::arrow_shared)
target_link_libraries(lakesoul_dataset PRIVATE liblakesoul_io_c)
set_target_properties(lakesoul_dataset PROPERTIES
    INSTALL_RPATH "@loader_path"
    BUILD_WITH_INSTALL_RPATH TRUE
)

install(TARGETS lakesoul_dataset DESTINATION ${SKBUILD_PROJECT_NAME})


# # add_library(libarrow SHARED IMPORTED)

# # set_target_properties(libarrow PROPERTIES
# #     IMPORTED_LOCATION "${PYARROW_DIR}/libarrow.so.${PYARROW_ABI_TAG}"
# #     INTERFACE_INCLUDE_DIRECTORIES "${PYARROW_DIR}/include"
# #     INTERFACE_COMPILE_DEFINITIONS "_GLIBCXX_USE_CXX11_ABI=1")

# # add_library(libarrow_dataset SHARED IMPORTED)
# # set_target_properties(libarrow_dataset PROPERTIES
# #     IMPORTED_LOCATION "${PYARROW_DIR}/libarrow_dataset.so.${PYARROW_ABI_TAG}"
# #     INTERFACE_INCLUDE_DIRECTORIES "${PYARROW_DIR}/include"
# #     INTERFACE_COMPILE_DEFINITIONS "_GLIBCXX_USE_CXX11_ABI=1"
# #     INTERFACE_LINK_LIBRARIES "libarrow")

# # add_library(libarrow_python SHARED IMPORTED)
# # set_target_properties(libarrow_python PROPERTIES
# #     IMPORTED_LOCATION "${PYARROW_DIR}/libarrow_python.so"
# #     INTERFACE_INCLUDE_DIRECTORIES "${NUMPY_INCLUDE_DIR};${PYTHON_INCLUDE_DIR}"
# #     INTERFACE_LINK_LIBRARIES "libarrow;libarrow_dataset")

# # # Generate .py from .proto
# # add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/python/lakesoul/metadata/generated/entity_pb2.py
# #                    COMMAND ${CMAKE_COMMAND} -E make_directory
# #                            ${PROJECT_BINARY_DIR}/python/lakesoul/metadata/generated
# #                    COMMAND ${VENV_PYTHON} -m grpc.tools.protoc
# #                            -I=${PROJECT_SOURCE_DIR}/../rust/proto/src
# #                            --python_out=${PROJECT_BINARY_DIR}/python/lakesoul/metadata/generated
# #                            ${PROJECT_SOURCE_DIR}/../rust/proto/src/entity.proto
# #                    MAIN_DEPENDENCY ${PROJECT_SOURCE_DIR}/../rust/proto/src/entity.proto)

# # # Generate .cpp from .pyx
# # add_custom_command(OUTPUT ${PROJECT_BINARY_DIR}/python/lakesoul/arrow/_lakesoul_dataset.cpp
# #                    COMMAND ${VENV_CYTHON} ${PROJECT_SOURCE_DIR}/../python/lakesoul/arrow/_lakesoul_dataset.pyx
# #                            --cplus -o ${PROJECT_BINARY_DIR}/python/lakesoul/arrow/_lakesoul_dataset.cpp
# #                    MAIN_DEPENDENCY ${PROJECT_SOURCE_DIR}/../python/lakesoul/arrow/_lakesoul_dataset.pyx
# #                    DEPENDS ${PROJECT_SOURCE_DIR}/../python/lakesoul/arrow/_lakesoul_dataset.pxd
# #                            ${PROJECT_SOURCE_DIR}/../python/lakesoul/arrow/_lakesoul_dataset_cpp.pxd
# #                            ${PROJECT_BINARY_DIR}/python/lakesoul/metadata/generated/entity_pb2.py)

# # Create Python extension module
# add_library(lakesoul_dataset SHARED
#             add.cpp
#             )
# # target_include_directories(lakesoul_dataset PRIVATE include)
# # set_target_properties(lakesoul_dataset PROPERTIES PREFIX "_")
