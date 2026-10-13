# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(SNAPPY_VERSION "1.3.1")

if (DEFINED ENV{SNAPPY_HOME})
  set (SNAPPY_HOME "$ENV{SNAPPY_HOME}")
endif ()

if (NOT "${SNAPPY_HOME}" STREQUAL "")
  find_package (Snappy REQUIRED)
  set(SNAPPY_VENDORED FALSE)
else ()
  set(CFLAGS "-g -gdwarf-4")
  if (CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
    # For snappy to detect NEON CRC, it need these flags
    SET(CFLAGS "${CFLAGS} -march=armv8-a+crc")
  endif()
  set(THIRDPARTY_DIR "${CMAKE_BINARY_DIR}/thirdparty")
  set(SNAPPY_HOME "${THIRDPARTY_DIR}/snappy_${SNAPPY_VERSION}-install")
  set(SNAPPY_INCLUDE_DIR "${SNAPPY_HOME}/include")
  set(SNAPPY_STATIC_LIB "${SNAPPY_HOME}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}snappy${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(SNAPPY_CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${SNAPPY_HOME}
        -DBUILD_SHARED_LIBS=OFF
        -DCMAKE_BUILD_TYPE=RELEASE
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DCMAKE_INSTALL_LIBDIR=lib
        -DSNAPPY_BUILD_TESTS=OFF
        -DSNAPPY_BUILD_BENCHMARKS=OFF
        -DSNAPPY_REQUIRE_AVX2=ON
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
        -DCMAKE_CXX_FLAGS=${CFLAGS}
        -DCMAKE_C_FLAGS=${CFLAGS}
        -DCMAKE_TOOLCHAIN_FILE=${CMAKE_SOURCE_DIR}/cmake_modules/clang_toolchain.cmake)

  ExternalProject_Add (snappy_ep
    URL "https://github.com/google/snappy/archive/${SNAPPY_VERSION}.tar.gz"
    CMAKE_ARGS ${SNAPPY_CMAKE_ARGS}
    ${THIRDPARTY_LOG_OPTIONS}
    BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}")

  set(SNAPPY_LIBRARY ${SNAPPY_STATIC_LIB})
  set(SNAPPY_VENDORED TRUE)
endif ()

add_library (impala_snappy INTERFACE)
add_library (impala::snappy ALIAS impala_snappy)
target_link_libraries(impala_snappy INTERFACE ${SNAPPY_STATIC_LIB})
target_include_directories (impala_snappy SYSTEM INTERFACE ${SNAPPY_INCLUDE_DIR})

if (SNAPPY_VENDORED)
  add_dependencies (impala_snappy snappy_ep)
  if (INSTALL_VENDORED_LIBS)
    install(FILES "${SNAPPY_STATIC_LIB}"
            DESTINATION "lib")
  endif ()
endif ()
