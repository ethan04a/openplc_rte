cmake_minimum_required(VERSION 3.10)
project(plc_application C)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ===================== 【新增】构建类型配置（Debug/Release） =====================
# 默认编译为Debug版本（带调试符号、无优化、GDB可用）
# 手动切换Release：cmake -DCMAKE_BUILD_TYPE=Release ..
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Build type (Debug/Release)" FORCE)
endif()

# ===================== 【新增】分模式编译选项 =====================
# 公共编译选项（所有模式都生效）
set(COMMON_FLAGS
    -Wall
    -Wextra
    -fstack-protector-strong
    -D_FORTIFY_SOURCE=2
    -Wformat
    -Wformat-security
    -fPIC
    -fPIE
)

# Debug模式：开启调试信息(-g)、关闭优化(-O0)、关闭警告报错
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(${COMMON_FLAGS} -g -O0 -Wno-error)
endif()

# Release模式：正式发布（优化、警告报错）
if(CMAKE_BUILD_TYPE STREQUAL "Release")
    add_compile_options(${COMMON_FLAGS} -O2 -Werror)
endif()

# Find Python development libraries
find_package(PkgConfig REQUIRED)
pkg_check_modules(PYTHON REQUIRED python3-embed)

# ===================== 头文件包含路径（放.h文件的地方） =====================
# 【你添加自定义.h头文件】：如果有新的头文件目录，直接在这里加一行
include_directories(
    ${CMAKE_SOURCE_DIR}/core/src/plc_app
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/utils
    ${CMAKE_SOURCE_DIR}/core/generated/plc_lib
    ${CMAKE_SOURCE_DIR}/core/generated/plc_lib/lib
    ${PYTHON_INCLUDE_DIRS}
    # 示例：添加你的头文件目录
    # ${CMAKE_SOURCE_DIR}/core/src/plc_app/your_module
)

# On Cygwin/MSYS2, disable treating warnings as errors
if(CMAKE_SYSTEM_NAME MATCHES "CYGWIN|MSYS")
    add_compile_options(-Wno-error)
endif()

# ===================== 可执行文件（放.c文件的地方） =====================
# 【你添加自定义.c源码】：所有.c文件都在这里添加！
add_executable(plc_main
    # 原生OpenPLC核心代码
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/plc_main.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/utils/log.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/utils/utils.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/utils/watchdog.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/image_tables.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/journal_buffer.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/plc_state_manager.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/plcapp_manager.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/scan_cycle_manager.c
    ${CMAKE_SOURCE_DIR}/core/src/drivers/plugin_driver.c
    ${CMAKE_SOURCE_DIR}/core/src/drivers/plugin_config.c
    ${CMAKE_SOURCE_DIR}/core/src/drivers/plugin_utils.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/unix_socket.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/debug_handler.c
    ${CMAKE_SOURCE_DIR}/core/src/plc_app/client_tcp_udp.c

    # ===================== 【你的自定义 .c 文件 从这里添加】 =====================
    # 示例（热冗余模块/你的C文件）：
    # ${CMAKE_SOURCE_DIR}/core/src/plc_app/redundancy/redundancy_heartbeat.c
    # ${CMAKE_SOURCE_DIR}/core/src/plc_app/redundancy/redundancy_sync.c
    # ${CMAKE_SOURCE_DIR}/core/src/plc_app/your_test.c
)

# 链接库
target_link_libraries(plc_main
    dl
    pthread
    ${PYTHON_LIBRARIES}
)

# 导出符号 + 输出目录
target_link_options(plc_main PRIVATE -rdynamic)
set_target_properties(plc_main PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
)