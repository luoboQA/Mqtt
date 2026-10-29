cmake_minimum_required(VERSION 3.13)

project(sol C)
set(VERSION 0.18.5)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(DEBUG "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(TSAN "Build with ThreadSanitizer" OFF)
option(COVERAGE "Build with gcov instrumentation" OFF)
option(FUZZ "Build the libFuzzer target for mqtt_unpack (requires clang)" OFF)
set(EVENTLOOP "default" CACHE STRING
    "Event loop backend: default, epoll, poll or select (explicit backends are Linux-only)")

add_definitions("-D_DEFAULT_SOURCE")

if (EVENTLOOP STREQUAL "epoll")
    add_definitions("-DSOL_EVENTLOOP_EPOLL")
elseif (EVENTLOOP STREQUAL "poll")
    add_definitions("-DSOL_EVENTLOOP_POLL")
elseif (EVENTLOOP STREQUAL "select")
    add_definitions("-DSOL_EVENTLOOP_SELECT")
endif ()

find_package(OpenSSL REQUIRED)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR})

# Explicit lists instead of file(GLOB): adding a source is a visible change,
# and a removed file cannot linger in an old build directory. The lists also
# document which translation units make up the broker.
set(SOURCES
    src/acl.c
    src/bst.c
    src/client.c
    src/config.c
    src/connect.c
    src/cron.c
    src/ev.c
    src/handlers.c
    src/iterator.c
    src/list.c
    src/logging.c
    src/memory.c
    src/memorypool.c
    src/mqtt.c
    src/network.c
    src/pack.c
    src/persistence.c
    src/publish.c
    src/server.c
    src/session.c
    src/sol.c
    src/subscribe.c
    src/subscriber.c
    src/topic.c
    src/topic_match.c
    src/topic_store.c
    src/trie.c
    src/util.c)

# The small unit suite covers the containers and shared low-level helpers.
set(STRUCT_TEST_SOURCES
    src/bst.c
    src/config.c
    src/iterator.c
    src/list.c
    src/logging.c
    src/memory.c
    src/trie.c
    src/util.c
    tests/unit/sol_test.c
    tests/unit/structures_test.c)

# The protocol tests are pure: mqtt_unpack(), the topic matcher and the
# configuration parser. No server, no event loop, no sockets.
set(PROTO_TEST_SOURCES
    src/config.c
    src/logging.c
    src/memory.c
    src/mqtt.c
    src/pack.c
    src/topic_match.c
    src/util.c
    tests/protocol/proto_test.c
    tests/protocol/mqtt_test.c
    tests/protocol/topic_match_test.c
    tests/protocol/config_test.c
    tests/protocol/util_test.c)

set(AUTHOR "Andrea Giacomo Baldan")
set(LICENSE "BSD2 license")

if (APPLE)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I/opt/homebrew/opt/openssl@3/include")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I/opt/homebrew/opt/openssl@3/include")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L/opt/homebrew/opt/openssl@3/lib")
endif (APPLE)

if (TSAN AND (DEBUG OR COVERAGE))
    message(FATAL_ERROR "TSAN cannot be combined with DEBUG or COVERAGE")
endif ()

if (TSAN)
    # ThreadSanitizer cannot be combined with the address/undefined sanitizers
    # of the DEBUG build (they are mutually exclusive), and the -pg profiling
    # instrumentation gets in its way: this is a build of its own
    message(STATUS "Configuring build for ThreadSanitizer")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wunused -Werror -pedantic \
    -Wno-unused-result -std=c11 -ggdb -O1 -fsanitize=thread")
elseif (DEBUG)
    message(STATUS "Configuring build for debug")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wunused -Werror -pedantic \
    -Wno-unused-result -std=c11 -ggdb -fsanitize=address \
    -fsanitize=undefined -fno-omit-frame-pointer -pg")
elseif (COVERAGE)
    message(STATUS "Configuring build for coverage")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wunused -Werror -pedantic \
    -Wno-unused-result -std=c11 -O0 -g --coverage")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} --coverage")
else ()
    message(STATUS "Configuring build for production")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wunused -Werror -pedantic \
    -Wno-unused-result -std=c11 -O3")
endif ()

if (APPLE)
    set(SOL_LIBS pthread ssl crypto)
    set(CRYPT_LIBS crypto)
else ()
    set(SOL_LIBS pthread ssl crypto crypt)
    set(CRYPT_LIBS crypt crypto)
endif ()

# Executable
add_executable(sol ${SOURCES})
target_link_libraries(sol ${SOL_LIBS})

# Unit tests
add_executable(sol_test ${STRUCT_TEST_SOURCES})
target_link_libraries(sol_test pthread ${CRYPT_LIBS})

add_executable(sol_proto_test ${PROTO_TEST_SOURCES})
target_link_libraries(sol_proto_test ${CRYPT_LIBS})

if (FUZZ)
    if (NOT CMAKE_C_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "FUZZ=ON requires clang (libFuzzer)")
    endif ()
    add_executable(sol_fuzz
        tests/fuzz/fuzz_unpack.c
        src/mqtt.c
        src/pack.c
        src/util.c
        src/logging.c
        src/memory.c)
    target_compile_options(sol_fuzz PRIVATE -fsanitize=fuzzer,address -g -O1)
    target_link_options(sol_fuzz PRIVATE -fsanitize=fuzzer,address)
endif ()

# CTest: the two unit suites plus the integration suite, which starts its own
# broker. SOL_BIN lets the Python helpers find the freshly built binary
# instead of relying on a ./sol symlink in the source tree.
enable_testing()
add_test(NAME unit COMMAND $<TARGET_FILE:sol_test>)
add_test(NAME proto COMMAND $<TARGET_FILE:sol_proto_test>)
add_test(NAME integration
    COMMAND ${CMAKE_COMMAND} -E env SOL_BIN=$<TARGET_FILE:sol>
            python3 ${CMAKE_SOURCE_DIR}/tests/integration/run.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

include(GNUInstallDirs)
install(TARGETS sol RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(FILES etc/conf/sol.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/sol)
install(FILES etc/systemd/sol.service
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/systemd/system)
