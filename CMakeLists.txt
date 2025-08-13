cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME lance)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(include)

# Build Rust library first
set(RUST_LIB_NAME lance_duckdb_ffi)

# Set Rust library path based on build type
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(RUST_BUILD_FLAG --release)
    set(RUST_TARGET_DIR release)
else()
    set(RUST_BUILD_FLAG)
    set(RUST_TARGET_DIR debug)
endif()

# Set Rust library path based on platform
if(WIN32)
    # Windows with MinGW typically generates .a files with lib prefix
    # Windows with MSVC typically generates .lib files without lib prefix
    if(MINGW)
        set(RUST_LIB_PATH ${CMAKE_CURRENT_SOURCE_DIR}/target/${RUST_TARGET_DIR}/lib${RUST_LIB_NAME}.a)
    else()
        set(RUST_LIB_PATH ${CMAKE_CURRENT_SOURCE_DIR}/target/${RUST_TARGET_DIR}/${RUST_LIB_NAME}.lib)
    endif()
else()
    # Unix-like systems (Linux, macOS)
    set(RUST_LIB_PATH ${CMAKE_CURRENT_SOURCE_DIR}/target/${RUST_TARGET_DIR}/lib${RUST_LIB_NAME}.a)
endif()

# Custom command to build Rust library
add_custom_command(
    OUTPUT ${RUST_LIB_PATH}
    COMMAND cargo build ${RUST_BUILD_FLAG} --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building Rust library..."
    VERBATIM
)

# Create a custom target for the Rust library
add_custom_target(rust_lib ALL DEPENDS ${RUST_LIB_PATH})

# Extension sources
set(EXTENSION_SOURCES 
    cpp/lance_extension.cpp
    cpp/lance_scan.cpp
    cpp/lance_copy.cpp
    cpp/lance_replacement.cpp
)

# Build static and loadable extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link Rust library
add_dependencies(${EXTENSION_NAME} rust_lib)
add_dependencies(${LOADABLE_EXTENSION_NAME} rust_lib)

target_link_libraries(${EXTENSION_NAME} ${RUST_LIB_PATH})
target_link_libraries(${LOADABLE_EXTENSION_NAME} ${RUST_LIB_PATH})

# Platform-specific compilation flags
if(WIN32 AND MINGW)
    # Enable big object files for MinGW to handle large unity builds
    add_compile_options(-Wa,-mbig-obj)
endif()

# Platform-specific linking
if(APPLE)
    target_link_libraries(${EXTENSION_NAME} 
        "-framework Security" 
        "-framework SystemConfiguration"
        "-framework CoreFoundation")
    target_link_libraries(${LOADABLE_EXTENSION_NAME} 
        "-framework Security" 
        "-framework SystemConfiguration"
        "-framework CoreFoundation")
elseif(WIN32)
    target_link_libraries(${EXTENSION_NAME} ws2_32 userenv bcrypt)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ws2_32 userenv bcrypt)
elseif(UNIX)
    target_link_libraries(${EXTENSION_NAME} pthread dl)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} pthread dl)
endif()

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)