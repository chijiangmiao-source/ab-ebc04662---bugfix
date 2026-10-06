# 星载回传分区交接调度服务

地面站切换接收实例时，保证**任一代次内一个分区至多归属一台实例**的调度服务。
核心是「撤销 — 确认 — 原子发布」交接协议，SQLite 单文件持久化，仅依赖 Python 3.11 标准库。

## 不变量

1. 撤销中的分区仍记在旧实例名下；新实例在旧实例确认前拿不到该分区，
   因而旧实例迟到的确认与新实例不会并行消费。
2. 旧实例确认后，以下动作在**同一个持久化事务**中完成：
   删除旧所有权 → 转授目标实例 → 推进可交接集合 → 公布新代次。
3. 任何读取只能看到两种状态：旧完整分配，或与已确认释放一致的中间分配。
   进程在释放后、发布前崩溃并重启，事务整体回滚，不会双重归属。
4. 确认过期（无进行中交接）、越权（非当前持有者）、含多余分区一律拒绝，且不推进代次。
5. 稳定请求标识幂等：相同请求重传返回首次结果；同一标识携带不同快照明确冲突（409）。
6. 交接进行中收到**后续完整成员快照**会被受理为排队（`202 queued`，至多一份），
   响应内带**已持久化目标**供调用方重新收敛；当前轮最后一次确认完成的同一事务内，
   排队快照基于已持久化的当前归属继续形成下一轮一致交接（确认响应以 `next_handover` 公布）。
   复用排队标识但成员不同、或排队槽已被另一份快照占用，一律明确冲突（409）；
   崩溃重启不会丢失已受理排队快照（启动时补做推广）。

目标分配完全由「成员标识 + 分区号」确定：`target = sorted(members)[int(part) % len(members)]`，
因此重算结果稳定且与顺序无关。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康端点，200 `{"status":"ok"}` |
| GET | `/v1/assignments[?member=]` | 当前分配读模型；`revoking` 字段标注正在撤销的目标 |
| GET | `/v1/handover` | 当前交接：持久化目标、待撤销、已释放集合 |
| POST | `/v1/snapshots` | `{"request_id","members"}` 提交完整成员快照 |
| POST | `/v1/confirms` | `{"request_id","member","parts"}` 旧实例确认撤销 |

状态码：`200` 稳定/已记录，`202` 已进入撤销，或已受理排队（`queued`），
`400` 参数或多余分区，`403` 越权确认，
`409` 确认过期 / 幂等冲突 / 排队槽已被另一份快照占用，`503` 存储不可用。

### 典型时序

```http
POST /v1/snapshots {"request_id":"r1","members":["a","b"]}   -> 200 stable
POST /v1/snapshots {"request_id":"r2","members":["b","c"]}   -> 202 revoking
POST /v1/snapshots {"request_id":"r3","members":["c","d"]}   -> 202 queued（撤销期间受理，至多一份）
GET  /v1/assignments?member=d                                -> 0 个分区（确认前拿不到）
POST /v1/confirms  {"request_id":"c1","member":"a","parts":["0","2","4"]}  -> 200 partially_released, epoch 1
POST /v1/confirms  {"request_id":"c2","member":"b","parts":["1","3","5"]}  -> 200 completed, epoch 2
                                                            # 同一事务内推广 r3：
                                                            # next_handover.status=revoking
GET  /v1/handover                                            -> active, request_id=r3
POST /v1/confirms  {"request_id":"c3","member":"b","parts":["0","2","4"]}  -> 200 partially_released, epoch 3
POST /v1/confirms  {"request_id":"c4","member":"c","parts":["1","3","5"]}  -> 200 completed, epoch 4
# r3 相同请求重传（同 request_id 同体）-> 原样返回首次（revoking）结果；
# 复用 r3 但成员不同、或排队槽已有快照时提交其它快照 -> 409
```

## 运行

### Docker Compose

```bash
docker compose up -d --build
curl http://127.0.0.1:8080/healthz

# 可配置宿主绑定与端口
HOST_BIND=0.0.0.0 HOST_PORT=9090 docker compose up -d
# 分区数
PARTITION_COUNT=1024 docker compose up -d
```

### 单次 verify 容器

围绕**三次连续成员快照的串联交接（含撤销期间受理与重启收敛）、失效确认、
中断恢复**运行规则测试、镜像构建自检与 API 冒烟，以状态码退出（0 成功）：

```bash
docker compose --profile verify run --rm verify
echo $?
```

### 本地直接运行（无需第三方依赖）

```bash
python verify.py                      # 与 verify 容器执行内容相同
PORT=8080 DB_PATH=./data/handoff.db python -m app.server
python -m unittest discover -s tests
```

## 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | 服务监听地址 |
| `PORT` | `8080` | 服务监听端口 |
| `DB_PATH` | `/data/handoff.db` | SQLite 持久化文件 |
| `PARTITION_COUNT` | `256` | 分区总数（首次初始化后固定） |
| `HOST_PORT` / `HOST_BIND` | `8080` / `127.0.0.1` | Compose 宿主端口与绑定地址 |

## 目录

```
app/store.py     持久化与交接协议（单事务原子发布、幂等、恢复）
app/server.py    标准库 HTTP 服务
verify.py        单次校验入口（规则测试 + 构建自检 + API 冒烟）
tests/           38 个规则/HTTP 测试，含连续快照串联、崩溃注入与跨连接并发
```
