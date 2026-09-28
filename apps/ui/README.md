# UI 入口

| 需求 | 代码目录 | 说明 |
|---|---|---|
| 原生桌面、K 线、盘口、下单、工作区与快捷键 | `desktop/`（Rust / eframe / egui） | [桌面契约](../../docs/VENUEFLOW.md) |
| 浏览器页面、邀请注册、KOL、Cookie 与 BFF | `web/`（Next.js / React） | [Web 指南](../../docs/WEB.md) |
| 同一 egui 界面的本机只读 WASM 预览 | `desktop/` 的 `web,preview` 特性 | [预览入口](../../docs/VENUEFLOW.md#浏览器终端本机预览) |

桌面开发启动使用 `scripts/Build-Run-VenueFlow.ps1`；Web 的 npm 命令在 `apps/ui/web` 执行。构建和发布统一遵守 [开发指南](../../docs/DEVELOPMENT.md)。

两端共享 `venue-control-protocol` 等明确协议，交易由 Control/Executor 执行。按需求选择对应客户端；WASM 预览不替代用户 Web，也不提供交易写入。

源码入口见 [CODEMAP](../../docs/CODEMAP.md)，机器人业务见 [带单同步](../../docs/LEADER_ORDER_MIRROR.md)、[Grid](../../docs/GRID_RUNTIME_REFACTOR.md)、[库存做市](../../docs/INVENTORY_MM.md)、[马丁做多](../../docs/SUPPORT_MARTINGALE.md)。
