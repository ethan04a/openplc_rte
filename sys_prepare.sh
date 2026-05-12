#!/bin/bash
# ==============================================
# 功能：硬件测试前置准备 - 输出完整系统信息
# 适用：CentOS/Ubuntu/Debian 等所有Linux发行版
# 用途：PLC热冗余测试前环境核查
# ==============================================

# 定义输出颜色（美观易读）
GREEN="\033[32m"
YELLOW="\033[33m"
BLUE="\033[34m"
RED="\033[31m"
CLEAR="\033[0m"

# 脚本标题
echo -e "${BLUE}=============================================${CLEAR}"
echo -e "${GREEN}        PLC测试前置准备 - 系统信息采集        ${CLEAR}"
echo -e "${BLUE}=============================================${CLEAR}"
echo ""

# 1. 基础信息
echo -e "${YELLOW}【1. 基础系统信息】${CLEAR}"
echo "主机名        : $(hostname)"
echo "当前时间      : $(date "+%Y-%m-%d %H:%M:%S")"
echo "当前登录用户  : $(whoami)"
echo "系统运行时间  : $(uptime -p)"
echo ""

# 2. 内核与系统版本
echo -e "${YELLOW}【2. 系统内核信息】${CLEAR}"
echo "Linux内核版本 : $(uname -r)"
echo "系统架构      : $(uname -m)"
# 兼容所有发行版读取系统版本
if [ -f /etc/os-release ]; then
    source /etc/os-release
    echo "系统发行版    : $PRETTY_NAME"
else
    echo "系统发行版    : 未知"
fi
echo ""

# 3. CPU信息
echo -e "${YELLOW}【3. CPU处理器信息】${CLEAR}"
echo "CPU型号       : $(grep 'model name' /proc/cpuinfo | head -1 | awk -F: '{print $2}' | sed 's/^ *//')"
echo "CPU核心数     : $(grep -c 'processor' /proc/cpuinfo)"
echo ""

# 4. 内存信息
echo -e "${YELLOW}【4. 内存使用信息】${CLEAR}"
free -h | awk 'NR==1{print "内存状态      : 总大小="$2," 已用="$3," 空闲="$4} NR==2{print "              : 可用="$7}'
echo ""

# 5. 磁盘信息
echo -e "${YELLOW}【5. 磁盘存储信息】${CLEAR}"
df -h | grep -E '^/dev/' | grep -v 'tmpfs' | awk '{print "磁盘分区:"$1," 总大小:"$2," 已用:"$3," 挂载点:"$6}'
echo ""

# 6. 网络信息
echo -e "${YELLOW}【6. 网络IP信息】${CLEAR}"
echo "本机IP地址    : $(hostname -I | awk '{print $1}')"
echo ""

# 结束提示
echo -e "${BLUE}=============================================${CLEAR}"
echo -e "${GREEN}          系统信息采集完成，可开始测试          ${CLEAR}"
echo -e "${BLUE}=============================================${CLEAR}"
echo ""