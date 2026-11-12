# 01-feedstitch（信汇）

Feedstitch — 多源目录/价目 Feed 归并落库：解析 → 业务键去重/冲突隔离 → 按源水位线增量或全量落库。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5100 |
| API | http://localhost:10100 |

## 主链

配置源与夹具 → 触发 Run → 解析 / 去重 / 冲突隔离 → Canonical + 运行报告对账

## RPA 录入队列（本机模拟）

“RPA 录入队列”页把一个夹具源逐行变成录入步骤队列（`rpa_batches` / `rpa_steps`），
状态为待执行 / 成功 / 失败，刷新页面后队列仍在（SQLite 持久化）：

1. 选源与去向（直接归并入库，或先进摄入前队列 `intake_rows`）→ 生成步骤队列，每行一步。
2. 执行入口支持单步、按批（N 步）、全部执行；成功步复用既有归并引擎，同键非键冲突仍进
   冲突隔离，失败步只记错误、绝不写入 staging / canonical / 冲突库冒充入库。
3. 每次执行同时产生一条普通 Run，Staging / 冲突 / Canonical 页可与 RPA 批次报告互证；
   报告数字全部源自步骤表并附带 `reconciles` 对账标志。
4. 选“摄入前队列”去向后，可再点“归并入库”把暂存行按同一套规则 promote。

仅模拟本机夹具录入，不驱动真实浏览器、不访问活网。

## 种子说明

- **Alpha Catalog（clean）**：JSON，三件干净货号，增量可落库。
- **Beta Offers（dirty）**：JSONL，含与 Alpha 同键但价格不同的冲突行，以及迟到低 revision 行（底座增量会因水位线跳过）。
- **Gamma Shelf（xml）**：简易 XML，两件独立货号。
- **Delta RPA Clicks（rpa）**：JSONL，两件新货号 + 一条与 Alpha 同键异价的冲突行 + 一条坏行（演示步骤失败闭环）。

打开“运行台”选源跑一次增量，再看 Staging / 冲突 / Canonical 三页计数是否与本次 Run 报告一致；
或在“RPA 录入队列”页对 Delta 生成队列并逐步 / 按批执行，核对批次报告与步骤列表、暂存 / 冲突 / 主数据数字同源。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现）

`lookback_backfill` / `schema_drift_guard` / `dead_letter_replay` / `webhook_push_source` /
`source_authority_rank` / `tombstone_retire` / `scd2_history` / `watermark_commit_gate` /
`field_map_transform` / `lineage_provenance` /
`fixture_dir_harvest` / `bronze_silver_promote` / `category_metric_mart` / `pii_mask_export`
