#!/bin/bash
set -e

# 启动后端服务（绑定到 127.0.0.1:39180）
/app/venue-control-server &
BACKEND_PID=$!

# 等待后端启动
sleep 2

# 启动 socat 代理，将 0.0.0.0:39181 转发到 127.0.0.1:39180
# 使用不同端口避免与 Docker 端口映射冲突
socat TCP-LISTEN:39181,bind=0.0.0.0,reuseaddr,fork TCP:127.0.0.1:39180 &
SOCAT_PID=$!

# 等待任意进程退出
wait -n $BACKEND_PID $SOCAT_PID

# 如果任一进程退出，终止所有进程
kill $BACKEND_PID $SOCAT_PID 2>/dev/null || true
wait
