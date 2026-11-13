# 行情接收台 · 顺序可靠投递演示

一个零运行时依赖的本地行情系统：服务端通过 WebSocket 推送多交易对报价（每个交易对**独立编号**），
接收端保证报价严格按序号顺序生效，并提供缺口补查、冲突留痕、原子落盘与重启恢复。

## 运行

要求 Node.js >= 21（用到内置 `WebSocket` / `fetch`）。

```bash
npm start            # 启动服务，默认 http://localhost:8080
# 可选环境变量：PORT、TICK_MS（推送间隔）、RETENTION（每交易对保留条数）
```

浏览器打开 `http://localhost:8080`，点击 **启动接收**。

### 测试

```bash
npm run test:unit    # 顺序引擎单测（内存存储 + 假 Socket，11 项）
npm run test:e2e     # 真实服务器端到端测试（真实 WS + HTTP 补查）
npm test             # 全部
```

## 目录结构

```
server/
  quote-server.cjs  HTTP 静态托管 + WebSocket 推送 + 补查接口 + 故障注入
  ws.cjs            零依赖 RFC6455 WebSocket 帧编解码
  data/             服务端自身状态落盘（git 忽略）
web/
  engine.js         顺序保证引擎（DOM 无关，浏览器与 Node 测试共用）
  storage-idb.js    IndexedDB 原子落盘适配器
  app.js / index.html / styles.css   接收界面
test/
  engine.test.js    引擎单测
  e2e.test.js       端到端测试
  helpers.js        内存存储 / 假 Socket 等测试夹具
```

## 顺序与可靠性保证（核心需求对照）

| 需求 | 实现方式 |
| --- | --- |
| 每交易对独立编号 | 服务端 `state[symbol].seq` 各自递增；引擎状态按 symbol 隔离 |
| 重复消息 | `seq <= applied` 直接丢弃，不重复生效；内容相同静默去重 |
| 乱序 / 后到旧消息 | 旧序号永不覆盖最新报价；超前序号进 `pending` 暂存 |
| 漏一段（重连丢消息） | 检测到序号缺口即暂存后续消息，调用 HTTP 补查接口，补齐后按序 drain |
| 实时与补查重叠只处理一次 | 暂存表与补查结果按 seq 合并，同 seq 比较内容，相同则只保留一条 |
| 同序号内容不同 | 记录 **conflict** 并在界面明确标出，保留已落盘版本，绝不悄悄二选一 |
| 补查记录已过期 | HTTP 410 → 该交易对标记 **无法恢复（unrecoverable）**，最新报价冻结、后续消息继续暂存；**其他交易对照常接收**。操作员可「承认缺口并跳过」继续 |
| 断网/重复/删历史操作 | 工具栏与详情页提供故障注入按钮（见下） |
| 关闭再打开继续用落盘进度 | IndexedDB 持久化进度与消息，`start()` 时水合恢复 |
| 消息与进度一起保存 | 单条 IndexedDB 事务同时写 message 与 progress；引擎 `await` 事务成功后才前移内存游标，杜绝「进度前移、记录未落盘」 |

## 消息为什么生效（界面可追溯）

每条落盘消息都带 `source` 与 `reason`：

- **锚定（anchor）**：新订阅锚定首条收到的序号，更早历史不回放
- **实时（live）**：序号连续到达，直接刷新最新报价
- **补查（backfill）**：该序号经 HTTP 补查返回，按序补齐缺口

详情页逐条展示序号、来源、价格与生效原因；冲突区展示同序号两份内容的完整 JSON 对比。

## 故障注入

顶部工具栏（全局）：

- **断开连接**：服务端强制关闭 WS，客户端指数退避自动重连，重连后按 `hello_ack` 位点补查漏段
- **断网 开/关**：服务端静默丢弃所有推送（模拟黑洞），恢复后同样靠补查补齐

交易对详情页（选中某交易对后）：

- **插入重复消息**：重发最新序号 ×2，验证去重
- **插入乱序消息**：重发上一条旧序号，验证不覆盖最新报价
- **注入同序号冲突**：同 seq 不同价格，验证冲突标记
- **删除 3 条历史**：从服务端保留窗口删除一段，新客户端补查该段时得到 410，演示无法恢复

## HTTP 接口

- `GET /api/backfill?symbol=&from=&to=`：按序号区间补查；完整返回 `200 {messages}`，
  部分已过期返回 `410 {error:"expired", earliestSeq, latestSeq, missing, messages}`
- `POST /api/admin/disconnect|network|duplicate|reorder|conflict|delete|state`：故障注入与状态查询

## WebSocket 协议

客户端连上后发 `{"type":"hello","cursors":{symbol:appliedSeq}}`，
服务端回 `hello_ack`（含各交易对 `earliestSeq/latestSeq` 位点），随后推送 `quote` 帧与 5s 一次的 `ping`。
