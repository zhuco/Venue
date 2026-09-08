#!/usr/bin/env bash
# Venue 本地开发启动脚本
# 用法：./scripts/dev-local.sh [start|stop|status|logs]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印信息
info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

# 检查依赖
check_dependencies() {
    info "检查依赖..."
    
    if ! command -v docker &> /dev/null; then
        error "Docker 未安装，请先安装 Docker"
        exit 1
    fi
    
    if ! docker compose version &> /dev/null; then
        error "Docker Compose 未安装，请先安装 Docker Compose"
        exit 1
    fi
    
    if ! command -v node &> /dev/null; then
        error "Node.js 未安装，请先安装 Node.js >= 22.18.0"
        exit 1
    fi
    
    if ! command -v npm &> /dev/null; then
        error "npm 未安装，请先安装 npm"
        exit 1
    fi
    
    # 检查 Node.js 版本
    NODE_VERSION=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
    if [ "$NODE_VERSION" -lt 22 ]; then
        error "Node.js 版本过低：$(node -v)，要求 >= 22.18.0"
        exit 1
    fi
    
    success "依赖检查通过"
}

# 启动后端服务（Docker）
start_backend() {
    info "启动后端服务（PostgreSQL + Control Server）..."
    
    cd "$PROJECT_ROOT"
    docker-compose up -d
    
    # 等待服务就绪
    info "等待服务启动..."
    sleep 5
    
    # 检查服务状态
    if docker-compose ps | grep -q "venue-postgres.*Up"; then
        success "PostgreSQL 已启动 (端口 5432)"
    else
        error "PostgreSQL 启动失败"
        docker-compose logs postgres
        exit 1
    fi
    
    if docker-compose ps | grep -q "venue-control-server.*Up"; then
        success "Control Server 已启动 (端口 39180)"
    else
        error "Control Server 启动失败"
        docker-compose logs control-server
        exit 1
    fi
}

# 启动前端开发服务器
start_frontend() {
    info "启动前端开发服务器..."
    
    cd "$PROJECT_ROOT/apps/ui/web"
    
    # 安装依赖（如果需要）
    if [ ! -d "node_modules" ]; then
        info "安装前端依赖..."
        npm ci
    fi
    
    # 设置环境变量
    export VENUE_CONTROL_ORIGIN="http://127.0.0.1:39180"
    : "${VENUE_WEB_SESSION_SIGNING_KEY:?Set a session signing key before starting the frontend}"
    
    info "启动 Next.js 开发服务器..."
    info "访问地址: http://localhost:3000"
    info "按 Ctrl+C 停止前端服务器"
    
    npm run dev
}

# 停止所有服务
stop_all() {
    info "停止所有服务..."
    
    # 停止前端（如果在运行）
    info "前端在启动它的终端使用 Ctrl+C 停止。"
    
    # 停止后端
    cd "$PROJECT_ROOT"
    if docker-compose ps | grep -q "Up"; then
        docker-compose down
        success "后端服务已停止"
    else
        info "后端服务未在运行"
    fi
}

# 显示服务状态
show_status() {
    echo ""
    echo "========================================="
    echo "Venue 本地开发环境状态"
    echo "========================================="
    echo ""
    
    # 后端状态
    echo "后端服务："
    cd "$PROJECT_ROOT"
    if docker-compose ps 2>/dev/null | grep -q "Up"; then
        docker-compose ps
        echo ""
        success "后端运行中"
    else
        warn "后端未运行"
    fi
    
    echo ""
    
    # 前端状态
    echo "前端服务："
    if pgrep -f "next dev" > /dev/null; then
        success "前端开发服务器运行中 (PID: $(pgrep -f 'next dev'))"
        echo "访问地址: http://localhost:3000"
    else
        warn "前端开发服务器未运行"
    fi
    
    echo ""
    echo "========================================="
}

# 查看日志
show_logs() {
    local service=${1:-all}
    
    cd "$PROJECT_ROOT"
    
    if [ "$service" = "backend" ]; then
        info "查看后端日志..."
        docker-compose logs -f
    elif [ "$service" = "frontend" ]; then
        warn "前端日志请直接查看前端终端输出"
    else
        info "查看所有服务日志..."
        docker-compose logs -f
    fi
}

# 主命令
case "${1:-start}" in
    start)
        check_dependencies
        start_backend
        echo ""
        info "后端服务已就绪，现在启动前端..."
        echo ""
        start_frontend
        ;;
    
    stop)
        stop_all
        ;;
    
    status)
        show_status
        ;;
    
    logs)
        show_logs "${2:-all}"
        ;;
    
    *)
        echo "用法: $0 [start|stop|status|logs]"
        echo ""
        echo "命令："
        echo "  start   启动所有服务（默认）"
        echo "  stop    停止所有服务"
        echo "  status  显示服务状态"
        echo "  logs    查看日志（可选参数：backend/frontend/all）"
        echo ""
        exit 1
        ;;
esac
