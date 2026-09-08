#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Venue Web Build Script
# 构建 Next.js standalone 产物并输出到 dist/ 目录
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEB_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$WEB_ROOT/dist"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }

# ============================================================================
# 第一步：检查 Node.js 版本
# ============================================================================
log_info "检查 Node.js 版本..."
if ! command -v node &> /dev/null; then
    log_error "Node.js 未安装，请先安装 Node.js >= 22.18.0"
    exit 1
fi

NODE_VERSION=$(node -v | sed 's/v//' | cut -d. -f1)
NODE_MAJOR=$(echo "$NODE_VERSION" | cut -d. -f1)
NODE_MINOR=$(echo "$NODE_VERSION" | cut -d. -f2)
NODE_PATCH=$(echo "$NODE_VERSION" | cut -d. -f3)

# 要求 >= 22.18.0
if [[ "$NODE_MAJOR" -lt 22 ]] || { [[ "$NODE_MAJOR" -eq 22 ]] && [[ "$NODE_MINOR" -lt 18 ]]; }; then
    log_error "Node.js 版本过低：$(node -v)，要求 >= 22.18.0"
    exit 1
fi
log_success "Node.js 版本：$(node -v)"

# ============================================================================
# 第二步：安装依赖
# ============================================================================
log_info "安装依赖..."
cd "$WEB_ROOT"

if [[ ! -f "package-lock.json" ]]; then
    log_warn "package-lock.json 不存在，执行完整安装..."
    npm install
else
    log_info "检测到 package-lock.json，执行快速安装..."
    npm ci
fi
log_success "依赖安装完成"

# ============================================================================
# 第三步：TypeScript 类型检查
# ============================================================================
log_info "执行 TypeScript 类型检查..."
npm run typecheck
log_success "类型检查通过"

# ============================================================================
# 第四步：构建 Next.js 应用
# ============================================================================
log_info "构建 Next.js 应用..."
npm run build
log_success "Next.js 构建完成"

# ============================================================================
# 第五步：安全边界验证
# ============================================================================
log_info "执行安全边界验证..."
npm run verify:boundary
log_success "安全边界验证通过"

# ============================================================================
# 第六步：打包产物到 dist/
# ============================================================================
log_info "打包产物到 dist/ 目录..."

# 清理旧的 dist 目录
if [[ -d "$DIST_DIR" ]]; then
    log_warn "清理旧的 dist 目录..."
    rm -rf "$DIST_DIR"
fi
mkdir -p "$DIST_DIR"

# 复制 standalone 产物
STANDALONE_DIR="$WEB_ROOT/.next/standalone"
if [[ ! -d "$STANDALONE_DIR" ]]; then
    log_error "standalone 目录不存在：$STANDALONE_DIR"
    exit 1
fi

log_info "复制 standalone 产物..."
cp -R "$STANDALONE_DIR"/. "$DIST_DIR"/

# 确保 .next/static 和 public 已复制（prepare-standalone.mjs 应该已经做了）
if [[ ! -d "$DIST_DIR/.next/static" ]]; then
    log_warn "static 资源未找到，手动复制..."
    mkdir -p "$DIST_DIR/.next/static"
    cp -R "$WEB_ROOT/.next/static"/. "$DIST_DIR/.next/static"/
fi

if [[ ! -d "$DIST_DIR/public" ]] && [[ -d "$WEB_ROOT/public" ]]; then
    log_warn "public 资源未找到，手动复制..."
    cp -r "$WEB_ROOT/public" "$DIST_DIR/public"
fi

# ============================================================================
# 第七步：生成部署说明
# ============================================================================
cat > "$DIST_DIR/DEPLOY.md" << 'EOF'
# Venue Web 部署说明

## 产物结构

```
dist/
├── server.js          # Node.js 服务入口
├── .next/
│   └── static/        # 静态资源（JS/CSS/图片）
├── public/            # 公共静态文件
└── node_modules/      # 运行时依赖（已精简）
```

## 部署步骤

### 1. 上传产物到服务器

```bash
# 方式一：直接复制
scp -r dist/ user@server:/opt/venue/web/

# 方式二：打包后上传
tar -czf venue-web.tar.gz dist/
scp venue-web.tar.gz user@server:/opt/venue/
ssh user@server "cd /opt/venue && tar -xzf venue-web.tar.gz && mv dist web"
```

### 2. 配置环境变量

在服务器上创建 `.env` 文件（或设置环境变量）：

```bash
# 必需：Control 服务地址
VENUE_CONTROL_ORIGIN=http://127.0.0.1:39180
VENUE_WEB_SESSION_SIGNING_KEY=replace-with-a-unique-random-secret

# 可选：监听端口（默认 3000）
PORT=3000

# 可选：主机名（默认 0.0.0.0）
HOSTNAME=0.0.0.0
```

### 3. 启动服务

```bash
cd /opt/venue/web
node server.js
```

### 4. 使用 systemd 管理（推荐）

创建 `/etc/systemd/system/venue-web.service`：

```ini
[Unit]
Description=Venue Web Service
After=network.target

[Service]
Type=simple
User=venue
WorkingDirectory=/opt/venue/web
Environment=VENUE_CONTROL_ORIGIN=http://127.0.0.1:39180
EnvironmentFile=/opt/venue/web/.env
ExecStart=/usr/bin/node server.js
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

启动服务：

```bash
sudo systemctl daemon-reload
sudo systemctl enable venue-web
sudo systemctl start venue-web
sudo systemctl status venue-web
```

### 5. 配置 Nginx 反向代理（可选）

```nginx
server {
    listen 80;
    server_name venue.example.com;

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
}
```

## 验证部署

```bash
# 检查服务状态
curl http://localhost:3000

# 查看日志
journalctl -u venue-web -f
```
EOF

log_success "部署说明已生成：$DIST_DIR/DEPLOY.md"

# ============================================================================
# 完成
# ============================================================================
echo ""
log_success "========================================="
log_success "构建完成！"
log_success "========================================="
echo ""
log_info "产物目录：$DIST_DIR"
log_info "产物大小：$(du -sh "$DIST_DIR" | cut -f1)"
echo ""
log_info "下一步操作："
echo "  1. 查看部署说明：cat $DIST_DIR/DEPLOY.md"
echo "  2. 本地测试：cd $DIST_DIR && node server.js"
echo "  3. 上传到服务器：scp -r $DIST_DIR user@server:/opt/venue/web/"
echo ""
