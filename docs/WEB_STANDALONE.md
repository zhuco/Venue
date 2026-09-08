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
