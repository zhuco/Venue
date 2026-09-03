# UI 入口

两套 UI 固定放在本目录同级位置，避免把桌面终端需求误改到用户 Web：

- `desktop/`：VenueFlow 原生桌面终端，Rust + eframe/egui。凡是桌面截图、K 线、盘口、交易对标签、下单面板、工作区布局与桌面快捷键，先从这里定位。
- `web/`：浏览器用户界面与同源 BFF，Next.js + React。凡是 URL 路由、Cookie 会话、邀请注册、KOL 落地页、响应式浏览器页面与 Web API，先从这里定位。

两端只共享 `crates/venue-control-protocol` 等明确协议，不复制交易规则、凭证或执行实现。目录改名或主要入口变化时同步更新根 `Cargo.toml`、`.github/workflows/workspace-gates.yml`、`docs/CODEMAP.md`、`docs/WEB.md` 与本文件。

## 远程 Control 的桌面启动

桌面端默认访问 `http://127.0.0.1:39180`。Control 按安全边界只监听服务器回环地址时，不要直接双击 UI；使用仓库根目录的 `scripts/Start-VenueFlow.ps1`，它会建立到服务器 `127.0.0.1:39180` 的 SSH 转发后再启动桌面端：

```powershell
G:\Venue\scripts\Start-VenueFlow.ps1
```

脚本只复用已有 SSH 凭证，不读取或保存 API Key、密码和会话令牌。已有本地 39180 监听时不会重复创建转发。
