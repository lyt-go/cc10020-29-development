# API集成与事件编排平台

面向第三方API接入、Webhook事件接收、消息队列消费与服务编排的全链路API数据集成平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/routers/multi_transform.py 多规则批量转换数据集接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/multi_transform.py 多规则批量转换（多选规则/版本/血缘/质检阻断）
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| API端点注册 | `api_endpoint` | 端点配置 | 端点编号、端点名称、请求方法 |
| Webhook通道 | `webhook_channel` | 通道配置 | 通道编号、通道名称、接收路径 |
| 事件结构定义 | `event_schema` | 事件结构 | 结构编号、事件类型、字段名称 |
| 消息队列消费 | `mq_consumer` | 消费配置 | 配置编号、队列名称、消费组名 |
| 死信队列管理 | `dlq_handler` | 死信记录 | 记录编号、原始队列、失败原因 |
| 认证凭证管理 | `api_auth_store` | 认证凭证 | 凭证编号、关联端点、凭证类型 |
| 流量控制 | `rate_limit_config` | 限流规则 | 规则编号、关联端点、请求间隔 |
| 数据转换编排 | `data_transform` | 转换规则 | 规则编号、源Schema、目标Schema |
| 事件分发路由 | `dispatch_rule` | 分发规则 | 规则编号、事件类型、目标系统 |
| 事件去重 | `dedup_filter` | 去重规则 | 规则编号、事件类型、去重键列 |
| 批量接入调度 | `batch_ingest` | 接入任务 | 任务编号、关联端点、Cron表达式 |
| 数据落湖 | `data_lake_sink` | 落湖配置 | 配置编号、数据来源、目标路径 |
| 接入质量校验 | `data_quality_rule` | 质量规则 | 规则编号、数据来源、校验字段 |
| 调用链路追踪 | `call_trace_log` | 调用记录 | 记录编号、关联端点、请求参数 |
| 增量水位管理 | `incre_cursor` | 水位标记 | 标记编号、关联端点、增量字段 |
| 历史回补任务 | `backfill_job` | 回补任务 | 任务编号、关联端点、起始日期 |
| 事件归档清理 | `event_archive` | 归档策略 | 策略编号、事件类型、归档条件 |
| 集成监控看板 | `integration_monitor` | 监控指标 | 指标编号、监控对象、成功率 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 多规则批量转换数据集

同一来源批次一次选择多套转换规则（多选规则），为每套规则生成独立的规则版本成品，
后端落在 `app/services/multi_transform.py`，前端页面为「多规则转换数据集」
（路由 `/multi-transform-datasets`）。核心约定：

- **幂等提交**：按 `批次编号 + 源数据指纹 + 规则集合指纹` 去重；同批同源重提只复用
  既有版本，不新增成品；数据或规则集合变化则拒绝覆盖。
- **断点续做**：转换逐行落盘（`multi_transform_row`），中断后 `resume` 从未完成
  版本的断点行继续，已完成的行与版本不重跑；源记录在批次级与行级双留存。
- **字段血缘**：每行成品记录目标字段 → 源路径/源值/转换函数/成品值，可按
  版本+源行号回查实际字段来源；字段映射支持 `{"目标字段": {"path": "...", "function": "..."}}`
  字段级转换函数。
- **分版本质检**：质量校验跨模块读取多规则数据集，按版本执行；阻断结论回写
  待复核清单（`multi_transform_review`），存在阻断待复核项的版本不可发布。
- **失败行补做**：映射修正后对同版本失败行原地重做，不产生新版本；补做后需重新质检。
- **跨映射对照**：按源行核对两套规则的成品字段差异与来源差异。

主要接口前缀 `/api/multi-transform`：`POST /batches`（提交）、
`POST /batches/{no}/run|resume`（执行/续做）、
`GET /batches/{no}/availability`（可领取版本与等待原因）、
`GET /batches/{no}/compare`（跨映射对照）、
`POST /versions/{id}/quality-check|publish|repair`（质检/发布/补做）、
`GET /versions/{id}/lineage`（血缘回查）。

领域：数据工程
子方向：数据采集、爬虫与RPA
