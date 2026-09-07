#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Venue 全栈打包脚本
# 支持 macOS/Windows 交叉编译到 Linux，或直接在 Linux 上编译
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$PROJECT_ROOT/dist"
RELEASE_ID="${RELEASE_ID:-$(date +%Y%m%d-%H%M%S)}"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }

# ============================================================================
# 环境检测
# ============================================================================
detect_environment() {
    log_step "检测编译环境..."
    
    OS="$(uname -s)"
    ARCH="$(uname -m)"
    
    case "$OS" in
        Linux)
            TARGET="x86_64-unknown-linux-gnu"
            BUILD_MODE="native"
            log_info "检测到 Linux 环境，使用本地编译"
            ;;
        Darwin)
            TARGET="x86_64-unknown-linux-gnu"
            BUILD_MODE="cross"
            log_info "检测到 macOS 环境，使用交叉编译"
            ;;
        MINGW*|MSYS*|CYGWIN*)
            TARGET="x86_64-unknown-linux-gnu"
            BUILD_MODE="cross"
            log_info "检测到 Windows 环境，使用交叉编译"
            ;;
        *)
            log_error "不支持的操作系统：$OS"
            exit 1
            ;;
    esac
    
    log_info "目标平台：$TARGET"
    log_info "编译模式：$BUILD_MODE"
}

# ============================================================================
# 检查交叉编译工具链
# ============================================================================
check_cross_compile_tools() {
    if [[ "$BUILD_MODE" == "native" ]]; then
        return 0
    fi
    
    log_step "检查交叉编译工具链..."
    
    local missing=()
    
    if ! command -v cargo-zigbuild &> /dev/null; then
        missing+=("cargo-zigbuild")
    fi
    
    if ! command -v zig &> /dev/null; then
        missing+=("zig")
    fi
    
    if ! rustup target list --installed | grep -q "x86_64-unknown-linux-gnu"; then
        missing+=("rust-target:x86_64-unknown-linux-gnu")
    fi
    
    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "缺少交叉编译工具："
        for tool in "${missing[@]}"; do
            echo "  - $tool"
        done
        echo ""
        log_info "安装指南："
        echo "  1. 安装 Zig: brew install zig (macOS) 或 https://ziglang.org/download/"
        echo "  2. 安装 cargo-zigbuild: cargo install cargo-zigbuild"
        echo "  3. 添加 Rust 目标：rustup target add x86_64-unknown-linux-gnu"
        exit 1
    fi
    
    log_success "交叉编译工具链检查通过"
}

# ============================================================================
# 检查基础工具
# ============================================================================
check_base_tools() {
    log_step "检查基础工具..."
    
    local missing=()
    
    if ! command -v cargo &> /dev/null; then
        missing+=("cargo")
    fi
    
    if ! command -v node &> /dev/null; then
        missing+=("node")
    fi
    
    if ! command -v npm &> /dev/null; then
        missing+=("npm")
    fi
    
    if [[ ${#missing[@]} -gt 0 ]]; then
        log_error "缺少基础工具："
        for tool in "${missing[@]}"; do
            echo "  - $tool"
        done
        exit 1
    fi
    
    log_success "基础工具检查通过"
}

# ============================================================================
# 编译后端 Rust 服务
# ============================================================================
build_backend() {
    log_step "编译后端 Rust 服务..."
    
    cd "$PROJECT_ROOT"
    
    local backend_dist="$DIST_DIR/backend"
    mkdir -p "$backend_dist"
    
    # 后端二进制列表
    local binaries=(
        "venue-control-server"
        "venue-executor-binance"
        "venue-leader-bot-admin"
        "venue-strategy-admin"
    )
    
    if [[ "$BUILD_MODE" == "cross" ]]; then
        log_info "使用 cargo-zigbuild 交叉编译..."
        
        for bin in "${binaries[@]}"; do
            log_info "编译 $bin..."
            cargo-zigbuild build \
                --release \
                --target "$TARGET" \
                --bin "$bin"
            
            # 复制产物
            local src="$PROJECT_ROOT/target/$TARGET/release/$bin"
            if [[ -f "$src" ]]; then
                cp "$src" "$backend_dist/"
                log_success "$bin 编译完成"
            else
                log_error "$bin 编译产物未找到：$src"
                exit 1
            fi
        done
    else
        log_info "使用 cargo 本地编译..."
        
        for bin in "${binaries[@]}"; do
            log_info "编译 $bin..."
            cargo build --release --bin "$bin"
            
            local src="$PROJECT_ROOT/target/release/$bin"
            if [[ -f "$src" ]]; then
                cp "$src" "$backend_dist/"
                log_success "$bin 编译完成"
            else
                log_error "$bin 编译产物未找到：$src"
                exit 1
            fi
        done
    fi
    
    # 生成后端版本信息
    cat > "$backend_dist/VERSION.json" << EOF
{
  "release_id": "$RELEASE_ID",
  "build_time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "target": "$TARGET",
  "build_mode": "$BUILD_MODE",
  "binaries": [
$(printf '    "%s"' "${binaries[0]}"; printf ',\n'; for bin in "${binaries[@]:1}"; do printf '    "%s"' "$bin"; printf ',\n'; done | sed '$ s/,$//')
  ]
}
EOF
    
    log_success "后端编译完成：$backend_dist"
}

# ============================================================================
# 编译前端 Web 应用
# ============================================================================
build_frontend() {
    log_step "编译前端 Web 应用..."
    
    cd "$PROJECT_ROOT/apps/ui/web"
    
    # 检查 Node 版本
    local node_version
    node_version=$(node -v | sed 's/v//' | cut -d. -f1)
    if [[ "$node_version" -lt 22 ]]; then
        log_error "Node.js 版本过低：$(node -v)，要求 >= 22.18.0"
        exit 1
    fi
    
    # 安装依赖
    log_info "安装前端依赖..."
    if [[ -f "package-lock.json" ]]; then
        npm ci
    else
        npm install
    fi
    
    # 类型检查
    log_info "执行 TypeScript 类型检查..."
    npm run typecheck
    
    # 构建
    log_info "构建 Next.js 应用..."
    npm run build
    
    # 安全边界验证
    log_info "执行安全边界验证..."
    npm run verify:boundary
    
    # 复制产物
    local frontend_dist="$DIST_DIR/frontend"
    mkdir -p "$frontend_dist"
    
    log_info "复制 standalone 产物..."
    cp -r .next/standalone/* "$frontend_dist/"
    
    # 确保静态资源已复制
    if [[ ! -d "$frontend_dist/.next/static" ]]; then
        mkdir -p "$frontend_dist/.next/static"
        cp -r .next/static/* "$frontend_dist/.next/static/"
    fi
    
    if [[ ! -d "$frontend_dist/public" ]] && [[ -d "public" ]]; then
        cp -r public "$frontend_dist/public"
    fi
    
    # 生成前端版本信息
    cat > "$frontend_dist/VERSION.json" << EOF
{
  "release_id": "$RELEASE_ID",
  "build_time": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "target": "node-standalone",
  "node_version": "$(node -v)"
}
EOF
    
    log_success "前端编译完成：$frontend_dist"
}

# ============================================================================
# 生成部署文档
# ============================================================================
generate_deploy_docs() {
    log_step "生成部署文档..."
    
    cat > "$DIST_DIR/DEPLOY.md" << 'EOF'
# Venue 全栈部署指南

## 产物结构

```
dist/
├── backend/              # 后端 Rust 服务
│   ├── venue-control-server      # Control HTTP/SSE 服务
│   ├── venue-executor-binance    # Binance 执行器
│   ├── venue-leader-bot-admin    # 带单管理工具
│   ├── venue-strategy-admin      # 策略管理工具
│   └── VERSION.json
├── frontend/             # 前端 Web 应用
│   ├── server.js                 # Node.js 服务入口
│   ├── .next/static/             # 静态资源
│   ├── public/                   # 公共文件
│   ├── node_modules/             # 运行时依赖
│   └── VERSION.json
└── DEPLOY.md             # 本文档
```

## 部署步骤

### 1. 上传产物到 Ubuntu 服务器

```bash
# 打包产物
cd dist
tar -czf venue-release-${RELEASE_ID}.tar.gz .

# 上传到服务器
scp venue-release-${RELEASE_ID}.tar.gz user@server:/opt/venue/

# 在服务器上解压
ssh user@server "cd /opt/venue && tar -xzf venue-release-${RELEASE_ID}.tar.gz"
```

### 2. 配置环境变量

创建 `/opt/venue/.env`：

```bash
# 数据库连接（必需）
DATABASE_URL=postgresql://venue:password@localhost:5432/venue

# Control 服务绑定地址
VENUE_CONTROL_BIND=127.0.0.1:39180

# 凭证加密密钥（32 字节 hex）
VENUE_CONTROL_CREDENTIAL_KEY=your-32-byte-hex-key

# 前端 BFF 连接的后端地址
VENUE_CONTROL_ENDPOINT=http://127.0.0.1:39180

# 前端监听端口
PORT=3000
```

### 3. 初始化数据库

```bash
# 首次部署需要安装 schema
cd /opt/venue/backend
./venue-control-server --install-schema
```

### 4. 启动后端服务

使用 systemd 管理后端服务：

创建 `/etc/systemd/system/venue-control.service`：

```ini
[Unit]
Description=Venue Control Server
After=network.target postgresql.service

[Service]
Type=simple
User=venue
WorkingDirectory=/opt/venue/backend
EnvironmentFile=/opt/venue/.env
ExecStart=/opt/venue/backend/venue-control-server
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

创建 `/etc/systemd/system/venue-executor.service`：

```ini
[Unit]
Description=Venue Executor Binance
After=network.target venue-control.service

[Service]
Type=simple
User=venue
WorkingDirectory=/opt/venue/backend
EnvironmentFile=/opt/venue/.env
ExecStart=/opt/venue/backend/venue-executor-binance
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable venue-control venue-executor
sudo systemctl start venue-control venue-executor
```

### 5. 启动前端服务

创建 `/etc/systemd/system/venue-web.service`：

```ini
[Unit]
Description=Venue Web Service
After=network.target venue-control.service

[Service]
Type=simple
User=venue
WorkingDirectory=/opt/venue/frontend
EnvironmentFile=/opt/venue/.env
ExecStart=/usr/bin/node server.js
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

启动服务：

```bash
sudo systemctl enable venue-web
sudo systemctl start venue-web
```

### 6. 配置 Nginx 反向代理

创建 `/etc/nginx/sites-available/venue`：

```nginx
server {
    listen 80;
    server_name venue.example.com;

    # 前端 BFF
    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }

    # Control API（可选，如果需要直接暴露）
    location /v2/ {
        proxy_pass http://127.0.0.1:39180;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
    }
}
```

启用配置：

```bash
sudo ln -s /etc/nginx/sites-available/venue /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## 验证部署

```bash
# 检查服务状态
sudo systemctl status venue-control venue-executor venue-web

# 检查端口
ss -tlnp | grep -E '3000|39180'

# 测试前端
curl http://localhost:3000

# 测试后端
curl http://localhost:39180/v2/ui/snapshot

# 查看日志
journalctl -u venue-control -f
journalctl -u venue-executor -f
journalctl -u venue-web -f
```

## 更新部署

```bash
# 1. 停止服务
sudo systemctl stop venue-web venue-executor venue-control

# 2. 备份旧版本
cd /opt/venue
mv backend backend.bak.$(date +%s)
mv frontend frontend.bak.$(date +%s)

# 3. 解压新版本
tar -xzf venue-release-new.tar.gz

# 4. 启动服务
sudo systemctl start venue-control venue-executor venue-web
```
EOF
    
    log_success "部署文档已生成：$DIST_DIR/DEPLOY.md"
}

# ============================================================================
# 主流程
# ============================================================================
main() {
    echo ""
    log_info "========================================="
    log_info "Venue 全栈打包脚本"
    log_info "Release ID: $RELEASE_ID"
    log_info "========================================="
    echo ""
    
    # 清理旧的 dist 目录
    if [[ -d "$DIST_DIR" ]]; then
        log_warn "清理旧的 dist 目录..."
        rm -rf "$DIST_DIR"
    fi
    mkdir -p "$DIST_DIR"
    
    # 环境检测
    detect_environment
    check_base_tools
    check_cross_compile_tools
    
    # 编译后端
    build_backend
    
    # 编译前端
    build_frontend
    
    # 生成部署文档
    generate_deploy_docs
    
    # 完成
    echo ""
    log_success "========================================="
    log_success "全栈打包完成！"
    log_success "========================================="
    echo ""
    log_info "产物目录：$DIST_DIR"
    log_info "产物大小：$(du -sh "$DIST_DIR" | cut -f1)"
    echo ""
    log_info "下一步操作："
    echo "  1. 查看部署文档：cat $DIST_DIR/DEPLOY.md"
    echo "  2. 打包产物：cd $DIST_DIR && tar -czf venue-release-$RELEASE_ID.tar.gz ."
    echo "  3. 上传到服务器：scp venue-release-$RELEASE_ID.tar.gz user@server:/opt/venue/"
    echo ""
}

main "$@"
