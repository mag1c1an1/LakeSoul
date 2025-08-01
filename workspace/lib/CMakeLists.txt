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
set(lakesoul_io_fullname liblakesoul_io_c${CMAKE_SHARED_LIBRARY_SUFFIX})

if(NOT EXISTS ${LAEKSOUL_SOURCE_DIR}/rust/target/release/${lakesoul_io_fullname})
    execute_process(COMMAND ${Cargo_EXECUTABLE} build -r
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/rust/lakesoul-io-c
        RESULT_VARIABLE rc)

    if(NOT rc EQUAL 0)
        message(FATAL_ERROR "Fail to build liblakesoul_io_c.so")
    endif()
endif()

# # Build liblakesoul_metadata_c.so
# if(NOT EXISTS ${PROJECT_SOURCE_DIR}/../rust/target/release/liblakesoul_metadata_c.so)
# execute_process(COMMAND ${Cargo_EXECUTABLE} build -r
# WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/../rust/lakesoul-metadata-c
# RESULT_VARIABLE rc)
# if(NOT rc EQUAL 0)
# message(FATAL_ERROR "Fail to build liblakesoul_metadata_c.so")
# endif()
# endif()

# message(STATUS "Arrow version: ${ARROW_VERSION}")
# message(STATUS "Arrow SO version: ${ARROW_FULL_SO_VERSION}")
add_library(liblakesoul_io_c SHARED IMPORTED)
set_target_properties(liblakesoul_io_c PROPERTIES
    IMPORTED_LOCATION "${LAEKSOUL_SOURCE_DIR}/rust/target/release/${lakesoul_io_fullname}"
    INTERFACE_INCLUDE_DIRECTORIES "${LAEKSOUL_SOURCE_DIR}/rust/lakesoul-io-c"
    IMPORTED_NO_SONAME TRUE)

add_library(lakesoul_dataset SHARED dataset.cpp fragment.cpp data_reader.cpp example.cpp)
target_include_directories(lakesoul_dataset PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_link_libraries(lakesoul_dataset PRIVATE arrow_shared arrow_dataset_shared arrow_python_shared)
target_link_libraries(lakesoul_dataset PUBLIC liblakesoul_io_c)
set_target_properties(lakesoul_dataset PROPERTIES
    INSTALL_RPATH "@loader_path/../pyarrow;${LAEKSOUL_SOURCE_DIR}/rust/target/release}"
    BUILD_WITH_INSTALL_RPATH TRUE
)

install(TARGETS lakesoul_dataset DESTINATION ${SKBUILD_PROJECT_NAME})
